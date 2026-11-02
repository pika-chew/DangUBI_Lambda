"""Kinesis 차량 데이터를 검증·가공해 S3에 보관하고 SQS로 전달하는 전처리 Lambda.

입력 메시지 (단말 원본, Base64로 인코딩된 JSON)
  {"vehicleId": "...", "ts": 생성 시각, "ty": 종류, "pld": {종류별 필드}}
  - ty=3 Trip: 시동이 꺼질 때 운행 하나를 요약해 한 번 보낸다.
  - ty=4 Microtrip: 주행 중 주기적으로 보낸다.
  - 그 밖의 ty는 처리 대상이 아니므로 로그만 남기고 건너뛴다(실패로 치지 않음).

출력 메시지 (SQS 본문, S3 저장 내용)
  {"vehicleId", "ts", "ty", "schemaVersion", "idempotencyKey", "pld": {PLD_FIELDS에 있는 필드만}}
  PLD_FIELDS에 없는 필드(연비·정비용 값, 휴대폰 번호, 위치 등)는 모두 버린다.
  남겨야 할 필드가 하나라도 없으면 그 레코드는 거부한다.

처리 흐름 (한 번의 호출 = Kinesis 레코드 배치 하나)
  1. 환경 변수(QUEUE_URL, PROCESSED_BUCKET, MAX_MESSAGE_BYTES)를 읽는다.
  2. 모든 레코드의 sequenceNumber를 먼저 확인한다. 하나라도 없으면 아무것도 쓰지 않고 호출 전체를 실패시킨다.
  3. 레코드마다
       a. Base64 디코딩 → 엄격한 JSON 검사 → 필드 선별 후 SQS 본문 생성 (message_body)
       b. 본문을 S3에 조건부 저장 (_archive)
       c. 저장에 성공한 본문을 모아 두었다가 10개 또는 256KiB 단위로 SQS에 일괄 전송 (_send)
     검사·저장·전송 중 하나라도 실패하면 그 레코드만 실패 목록에 넣고 다음 레코드를 처리한다.
  4. 실패한 sequenceNumber를 batchItemFailures로 돌려준다. Kinesis는 실패한 레코드만 다시 전달한다.

남은 실행 시간이 MIN_REMAINING_MS보다 적으면 새로 쓰지 않고 실패로 돌려 다음 호출에서 재시도한다.
Lambda 진입점은 맨 아래의 handler()다.
"""
import base64
import hashlib
import json
import math
import os
from typing import NamedTuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# SQS 메시지 하나와 SendMessageBatch 요청 하나의 최대 크기.
MAX_BATCH_BYTES = 262144
# 이보다 실행 시간이 적게 남으면 새 S3/SQS 쓰기를 하지 않는다.
MIN_REMAINING_MS = 12000
# 호출 하나가 Lambda 제한 시간을 넘기지 않도록 연결 2초, 읽기 3초, 최대 2회 시도로 제한한다.
CLIENT_CONFIG = Config(connect_timeout=2, read_timeout=3, retries={"max_attempts": 2, "mode": "standard"})

SCHEMA_VERSION = 1
# ty별로 pld에서 남길 필드와 출력 순서. 여기 없는 필드는 모두 버리고, 여기 있는 필드가 없으면 거부한다.
PLD_FIELDS = {
    3: ("tid", "stt", "edt", "dis", "hsts", "mesp", "dtvt"),  # Trip
    4: ("tid", "clt", "sp", "em"),  # Microtrip
}


class UnsupportedType(Exception):
    """처리 대상이 아닌 ty. 재시도해도 결과가 같으므로 실패가 아니라 건너뛴다. args[0]은 로그용 ty다."""


class Pending(NamedTuple):
    """S3 저장을 마치고 SQS 전송을 기다리는 레코드."""

    sequence: str
    body: str


class PreprocessHandler:
    """Kinesis 배치 하나를 처리한다.

    S3/SQS 클라이언트와 환경 변수를 생성자로 받아서, 테스트에서는 가짜 객체로 바꿔 끼울 수 있다.
    """

    def __init__(self, s3, sqs, environment):
        self._s3 = s3
        self._sqs = sqs
        self._environment = environment

    def handle_request(self, event, context):
        """Kinesis 이벤트를 처리하고 실패한 레코드 목록을 반환한다 (모듈 docstring의 처리 흐름 참고)."""
        queue_url = self._setting("QUEUE_URL")
        bucket = self._setting("PROCESSED_BUCKET")
        max_bytes = min(int(self._setting("MAX_MESSAGE_BYTES")), MAX_BATCH_BYTES)
        if max_bytes <= 0:
            raise ValueError("Invalid message limit")
        records = _object(event).get("Records")
        if not isinstance(records, list):
            raise ValueError("Expected a record list")
        # 체크포인트 식별자(sequenceNumber)가 하나라도 없으면 쓰기 전에 호출 전체를 실패시킨다.
        sequences = [_text(_object(_object(item).get("kinesis")), "sequenceNumber") for item in records]

        failed = set()
        skipped = 0
        pending = []
        pending_bytes = 0
        for record, sequence in zip(records, sequences):
            # 1) 검사와 본문 생성. 잘못된 레코드는 실패로, 대상이 아닌 ty는 건너뛰고 이웃 레코드는 계속 처리한다.
            try:
                body = message_body(record)
                encoded = body.encode("utf-8")
                if len(encoded) > max_bytes:
                    raise ValueError("Message exceeds queue limit")
            except UnsupportedType as exception:
                skipped += 1
                _log("record_skipped", sequenceNumber=sequence, ty=exception.args[0])
                continue
            except Exception as exception:
                failed.add(sequence)
                _log("record_rejected", sequenceNumber=sequence, error=type(exception).__name__)
                continue

            # 2) 이 레코드를 더하면 SQS 배치 제한(10개, 256KiB)을 넘기는 경우, 모아 둔 것을 먼저 보낸다.
            if pending and (len(pending) == 10 or pending_bytes + len(encoded) > MAX_BATCH_BYTES):
                failed |= self._send(pending, queue_url, context)
                pending = []
                pending_bytes = 0
            # 3) 시간이 부족하면 S3에도 쓰지 않고 재시도로 넘긴다.
            if context.get_remaining_time_in_millis() < MIN_REMAINING_MS:
                failed.add(sequence)
                _log("forward_deferred", count=1, reason="time_budget")
                continue
            # 4) S3 보관. 실패한 레코드는 SQS로 보내지 않는다.
            try:
                self._archive(record, encoded, bucket)
            except Exception as exception:
                failed.add(sequence)
                _log("record_archive_failed", sequenceNumber=sequence, error=type(exception).__name__)
                continue
            pending.append(Pending(sequence, body))
            pending_bytes += len(encoded)
        # 5) 남은 레코드를 전송하고, 원래 순서대로 실패 목록을 만든다.
        if pending:
            failed |= self._send(pending, queue_url, context)
        _log("processing_summary", received=len(records), skipped=skipped, failed=len(failed))
        return {"batchItemFailures": [{"itemIdentifier": sequence} for sequence in sequences if sequence in failed]}

    def _archive(self, record, body, bucket):
        """SQS로 보낼 본문과 같은 바이트를 S3에 저장한다.

        키는 processed/{id 앞 2자}/{idempotencyKey}/{본문 SHA-256}.json이다.
        If-None-Match: *로 쓰기 때문에, 재시도로 같은 객체가 이미 있으면(412) 성공으로 본다.
        """
        identity = _identity(record)
        key = f"processed/{identity[:2]}/{identity}/{_hash(body)}.json"
        try:
            self._s3.put_object(Bucket=bucket, Key=key, Body=body,
                                ContentType="application/json; charset=utf-8", IfNoneMatch="*")
        except ClientError as exception:
            status = exception.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status != 412 or exception.response.get("Error", {}).get("Code") != "PreconditionFailed":
                raise

    def _send(self, pending, queue_url, context):
        """모아 둔 레코드를 SendMessageBatch 한 번으로 보내고, 성공이 확인되지 않은 sequenceNumber를 반환한다.

        응답의 Successful에 들어 있는 항목만 성공이다. 예외가 나면 배치 전체를 실패로 본다.
        """
        failed = {item.sequence for item in pending}
        if context.get_remaining_time_in_millis() < MIN_REMAINING_MS:
            _log("forward_deferred", count=len(pending), reason="time_budget")
            return failed
        entries = [{"Id": str(index), "MessageBody": item.body} for index, item in enumerate(pending)]
        try:
            response = self._sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
            confirmed = {success.get("Id") for success in response.get("Successful", [])}
            failed = {item.sequence for index, item in enumerate(pending) if str(index) not in confirmed}
            if failed:
                _log("forward_batch_failed", failed=len(failed))
        except Exception as exception:
            _log("forward_batch_failed", failed=len(failed), error=type(exception).__name__)
        return failed

    def _setting(self, name):
        """필수 환경 변수를 읽는다. 없거나 비어 있으면 호출 전체를 실패시킨다."""
        value = self._environment.get(name)
        if value is None or not value.strip():
            raise RuntimeError(f"Missing setting: {name}")
        return value


def message_body(record):
    """Kinesis 레코드 하나를 검증·가공해 SQS 메시지 본문(JSON 문자열)을 만든다.

    data는 Base64로 인코딩된 UTF-8 JSON 객체여야 한다. 아래 경우는 ValueError로 거부한다.
    잘못된 Base64/UTF-8, 객체가 아닌 최상위 값, 중복 키, NaN·Infinity·범위를 넘는 실수, 뒤에 붙은 토큰,
    vehicleId·ts·ty·pld 또는 PLD_FIELDS의 필드 누락.
    ty가 PLD_FIELDS에 없으면 UnsupportedType을 던진다.
    """
    source = _object(record.get("kinesis"))
    decoded = base64.b64decode(_text(source, "data"), validate=True)
    message = json.loads(decoded.decode("utf-8"), object_pairs_hook=_unique_object,
                         parse_float=_finite_float, parse_constant=_reject_constant)
    if not isinstance(message, dict):
        raise ValueError("Expected a JSON object")
    ty = _required(message, "ty")
    # 3.0이나 True처럼 3·4와 같다고 비교되는 값이 통과하지 않도록 int만 받는다.
    if type(ty) is not int or ty not in PLD_FIELDS:
        raise UnsupportedType(ty if type(ty) is int else None)
    pld = _object(message.get("pld"))
    # 키 순서를 고정해야 재시도나 다른 프로세스에서도 같은 바이트(같은 S3 키)가 나온다.
    return _dumps({
        "vehicleId": _text(message, "vehicleId"),
        "ts": _required(message, "ts"),
        "ty": ty,
        "schemaVersion": SCHEMA_VERSION,
        "idempotencyKey": _identity(record),
        "pld": {field: _required(pld, field) for field in PLD_FIELDS[ty]},
    })


def _identity(record):
    """레코드 고유 ID. SHA-256(stream ARN + ':' + eventID). 받는 쪽은 이 값으로 중복을 걸러야 한다."""
    return _hash(f"{_text(record, 'eventSourceARN')}:{_text(record, 'eventID')}".encode("utf-8"))


def _hash(value):
    """바이트열의 SHA-256 16진수 문자열."""
    return hashlib.sha256(value).hexdigest()


def _object(value):
    """값이 dict(JSON 객체)인지 확인한다."""
    if not isinstance(value, dict):
        raise ValueError("Expected an object")
    return value


def _text(value, key):
    """value[key]가 비어 있지 않은 문자열인지 확인하고 반환한다."""
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ValueError(f"Missing string field: {key}")
    return result


def _required(value, key):
    """value[key]가 있고 null이 아닌지 확인하고 반환한다."""
    if value.get(key) is None:
        raise ValueError(f"Missing field: {key}")
    return value[key]


def _unique_object(pairs):
    """json.loads 훅. 같은 객체 안에 중복 키가 있으면 거부한다 (기본 동작은 마지막 값으로 덮어씀)."""
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate key")
        result[key] = value
    return result


def _finite_float(text):
    """json.loads 훅. 1e400처럼 무한대가 되는 실수를 거부한다."""
    value = float(text)
    if not math.isfinite(value):
        raise ValueError("Non-finite number")
    return value


def _reject_constant(name):
    """json.loads 훅. 표준 JSON이 아닌 NaN, Infinity, -Infinity를 거부한다."""
    raise ValueError(f"Non-finite number: {name}")


def _dumps(value):
    """Java(Jackson) 버전과 같은 형식으로 직렬화한다. 공백 없이, 한글은 이스케이프하지 않는다."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _log(event, **fields):
    """CloudWatch용 JSON 한 줄 로그. payload나 예외 메시지는 넣지 않는다."""
    print(_dumps({**fields, "event": event}), flush=True)


# 실행 환경이 재사용될 때 클라이언트를 다시 만들지 않도록 캐시한다.
_default = None


def handler(event, context):
    """Lambda 진입점 (preprocess.handler). 첫 호출 때 실제 S3/SQS 클라이언트를 만들고 이후에는 재사용한다."""
    global _default
    if _default is None:
        _default = PreprocessHandler(boto3.client("s3", config=CLIENT_CONFIG),
                                     boto3.client("sqs", config=CLIENT_CONFIG), os.environ)
    return _default.handle_request(event, context)
