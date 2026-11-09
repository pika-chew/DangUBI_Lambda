"""IoT Core 차량 메시지를 검증·가공해 Firehose로 전달하는 전처리 Lambda.

전체 흐름: IoT Core → (이 Lambda) → Firehose → S3 → SQS
S3 저장과 SQS 알림은 Firehose와 S3 이벤트 알림이 맡으므로 여기서는 Firehose에만 쓴다.

입력 이벤트 (IoT Rule이 호출, 호출 하나 = 메시지 하나)
  {"vehicleId": "...", "ts": 생성 시각, "ty": 종류, "pld": {종류별 필드}}
  - ty=3 Trip: 시동이 꺼질 때 운행 하나를 요약해 한 번 보낸다.
  - ty=4 Microtrip: 주행 중 주기적으로 보낸다.
  - 그 밖의 ty는 처리 대상이 아니므로 로그만 남기고 건너뛴다(실패로 치지 않음).

출력 레코드 (Firehose, 줄바꿈으로 끝나는 JSON 한 줄)
  {"vehicleId", "ts", "ty", "schemaVersion", "idempotencyKey", "pld": {PLD_FIELDS에 있는 필드만}}
  PLD_FIELDS에 없는 필드(연비·정비용 값, 휴대폰 번호, 위치 등)는 모두 버린다.
  남겨야 할 필드가 하나라도 없으면 그 메시지는 거부한다.

실패 처리
  검증이나 전송에 실패하면 예외를 던진다. 비동기 호출이므로 Lambda가 재시도하고,
  재시도까지 실패한 이벤트는 Lambda에 설정한 실패 목적지로 간다.

Lambda 진입점은 맨 아래의 handler()다.
"""
import hashlib
import json
import os

import boto3
from botocore.config import Config

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


class RecordFailed(Exception):
    """호출을 실패시켜 재시도하게 한다. 런타임이 메시지를 로그에 남기므로 원인 예외의 이름만 담는다."""


class PreprocessHandler:
    """메시지 하나를 처리한다.

    Firehose 클라이언트와 환경 변수를 생성자로 받아서, 테스트에서는 가짜 객체로 바꿔 끼울 수 있다.
    """

    def __init__(self, firehose, environment):
        self._firehose = firehose
        self._environment = environment

    def handle_request(self, event):
        """이벤트를 검증·가공해 Firehose로 보내고 처리 결과를 반환한다. 실패하면 RecordFailed를 던진다."""
        stream = self._setting("FIREHOSE_STREAM_NAME")
        try:
            body = message_body(event)
        except UnsupportedType as exception:
            _log("record_skipped", ty=exception.args[0])
            _summary(skipped=1)
            return {"status": "skipped"}
        except Exception as exception:
            raise _failure("record_rejected", exception) from None

        try:
            # Firehose는 레코드를 구분자 없이 이어 붙이므로, S3 객체가 한 줄에 JSON 하나가 되도록 줄바꿈을 붙인다.
            self._firehose.put_record(DeliveryStreamName=stream, Record={"Data": f"{body}\n".encode("utf-8")})
        except Exception as exception:
            raise _failure("forward_failed", exception) from None
        _summary()
        return {"status": "forwarded"}

    def _setting(self, name):
        """필수 환경 변수를 읽는다. 없거나 비어 있으면 호출 전체를 실패시킨다."""
        value = self._environment.get(name)
        if value is None or not value.strip():
            raise RuntimeError(f"Missing setting: {name}")
        return value


def message_body(event):
    """IoT 이벤트 하나를 검증·가공해 Firehose 레코드 본문(JSON 문자열)을 만든다.

    아래 경우는 ValueError로 거부한다.
    객체가 아닌 이벤트, vehicleId·ts·ty·pld 또는 PLD_FIELDS의 필드 누락, NaN·Infinity 값.
    ty가 PLD_FIELDS에 없으면 UnsupportedType을 던진다.
    """
    message = _object(event)
    ty = _required(message, "ty")
    # 3.0이나 True처럼 3·4와 같다고 비교되는 값이 통과하지 않도록 int만 받는다.
    if type(ty) is not int or ty not in PLD_FIELDS:
        raise UnsupportedType(ty if type(ty) is int else None)
    pld = _object(message.get("pld"))
    vehicle_id = _text(message, "vehicleId")
    ts = _required(message, "ts")
    kept = {field: _required(pld, field) for field in PLD_FIELDS[ty]}
    # 키 순서를 고정해야 재시도나 다른 프로세스에서도 같은 바이트(같은 idempotencyKey)가 나온다.
    return _dumps({
        "vehicleId": vehicle_id,
        "ts": ts,
        "ty": ty,
        "schemaVersion": SCHEMA_VERSION,
        "idempotencyKey": _identity(vehicle_id, ts, ty, kept),
        "pld": kept,
    })


def _identity(vehicle_id, ts, ty, pld):
    """메시지 고유 ID. 재전송된 메시지도 같은 값이 나오도록 내용으로 만든다. 받는 쪽은 이 값으로 중복을 걸러야 한다."""
    return hashlib.sha256(_dumps({"vehicleId": vehicle_id, "ts": ts, "ty": ty, "pld": pld}).encode("utf-8")).hexdigest()


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


def _dumps(value):
    """공백 없이, 한글은 이스케이프하지 않고 직렬화한다. NaN·Infinity는 ValueError가 난다."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _failure(event, exception):
    """실패를 기록하고 호출을 실패시킬 예외를 만든다. 호출하는 쪽에서 from None으로 원인 예외를 숨긴다."""
    name = type(exception).__name__
    _log(event, error=name)
    _summary(failed=1)
    return RecordFailed(name)


def _summary(skipped=0, failed=0):
    """호출 결과 로그. CloudWatch 지표 필터 ProcessingFailedRecords가 failed 값을 읽는다."""
    _log("processing_summary", received=1, skipped=skipped, failed=failed)


def _log(event, **fields):
    """CloudWatch용 JSON 한 줄 로그. payload나 예외 메시지는 넣지 않는다."""
    print(_dumps({**fields, "event": event}), flush=True)


# 실행 환경이 재사용될 때 클라이언트를 다시 만들지 않도록 캐시한다.
_default = None


def handler(event, context):
    """Lambda 진입점 (preprocess.handler). 첫 호출 때 Firehose 클라이언트를 만들고 이후에는 재사용한다."""
    global _default
    if _default is None:
        _default = PreprocessHandler(boto3.client("firehose", config=CLIENT_CONFIG), os.environ)
    return _default.handle_request(event)
