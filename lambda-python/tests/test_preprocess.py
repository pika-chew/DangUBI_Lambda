import base64
import copy
import json

import pytest
from botocore.exceptions import ClientError

from preprocess import PLD_FIELDS, PreprocessHandler, message_body

# 단말이 보내는 원본. 제외 필드까지 모두 들어 있다.
TRIP = {"vehicleId": "vehicle-001", "ts": 1793577600100, "ty": 3, "pld": {
    "tid": 354, "stt": 1793574000000, "edt": 1793577600000, "dis": 30000, "tdis": 123456789, "fc": 2100,
    "stlat": 37.509141, "stlon": 127.063228, "edlat": 37.510296, "edlon": 127.062512, "ctp": 88, "coe": 5000,
    "fct": 120, "hsts": 90, "mesp": 30, "idt": 300, "btv": 12.6, "gnv": 14.2, "wut": 60, "usm": "01012345678",
    "est": 600, "fwv": "1.2.3", "dtvt": 3600}}
MICROTRIP = {"vehicleId": "vehicle-001", "ts": 1793575800100, "ty": 4, "pld": {
    "tid": 354, "fc": 12, "lat": 37.509141, "lon": 127.063228, "lc": 80, "clt": 1793575800000, "cdit": 15000,
    "rpm": 2200, "sp": 70, "em": "28", "el": 40, "xyz": {"ax": 0.1, "ay": 0.2, "az": 9.8}, "vv": 12.4, "tpos": 20}}
# record(1)의 idempotencyKey. Java 버전과 같은 값이다.
KEY_1 = "e0b73fd498fce0b4eaceae376d06d3d3a189e9ffdd11fda4d26583df9f0a9462"


class FakeS3:
    def __init__(self, calls):
        self.calls = calls
        self.requests = []
        self.effects = []

    def put_object(self, **kwargs):
        self.calls.append("s3")
        self.requests.append(kwargs)
        if self.effects:
            effect = self.effects.pop(0)
            if effect is not None:
                raise effect
        return {}


class FakeSqs:
    def __init__(self, calls):
        self.calls = calls
        self.requests = []
        self.effects = []

    def send_message_batch(self, **kwargs):
        self.calls.append("sqs")
        self.requests.append(kwargs)
        if self.effects:
            effect = self.effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
            return effect
        return {"Successful": [{"Id": entry["Id"]} for entry in kwargs["Entries"]]}


class FakeContext:
    def __init__(self, *remaining):
        self.remaining = list(remaining)

    def get_remaining_time_in_millis(self):
        return self.remaining.pop(0) if len(self.remaining) > 1 else self.remaining[0]


def client_error(status, code):
    return ClientError({"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}, "PutObject")


@pytest.fixture
def env():
    calls = []
    environment = {"QUEUE_URL": "https://example.test/queue",
                   "PROCESSED_BUCKET": "test-processed", "MAX_MESSAGE_BYTES": "262144"}
    s3 = FakeS3(calls)
    sqs = FakeSqs(calls)
    handler = PreprocessHandler(s3, sqs, environment)

    class Env:
        pass

    result = Env()
    result.calls, result.environment, result.s3, result.sqs = calls, environment, s3, sqs
    result.context = FakeContext(30000)
    result.invoke = lambda records: handler.handle_request({"Records": records}, result.context)
    result.bodies = lambda: [entry["MessageBody"] for request in sqs.requests for entry in request["Entries"]]
    return result


def message(base=MICROTRIP, **changes):
    """원본 메시지를 복사해 최상위 필드를 바꾼다. 값이 None이면 그 필드를 지운다."""
    result = copy.deepcopy(base)
    for key, value in changes.items():
        if value is None:
            result.pop(key, None)
        else:
            result[key] = value
    return json.dumps(result, ensure_ascii=False)


def record(sequence, body=None):
    body = message() if body is None else body
    data = body.encode("utf-8") if isinstance(body, str) else body
    return {"eventSourceARN": "arn:aws:kinesis:ap-northeast-2:000000000000:stream/test",
            "eventID": f"shardId-000000000000:{sequence}",
            "kinesis": {"sequenceNumber": str(sequence), "partitionKey": "vehicle-001",
                        "approximateArrivalTimestamp": 1793240000,
                        "data": base64.b64encode(data).decode("ascii")}}


def failures(*sequences):
    return {"batchItemFailures": [{"itemIdentifier": sequence} for sequence in sequences]}


# --- 필드 선별 -------------------------------------------------------------

def test_trip_keeps_only_listed_fields():
    assert json.loads(message_body(record(1, message(TRIP)))) == {
        "vehicleId": "vehicle-001", "ts": 1793577600100, "ty": 3, "schemaVersion": 1, "idempotencyKey": KEY_1,
        "pld": {"tid": 354, "stt": 1793574000000, "edt": 1793577600000, "dis": 30000,
                "hsts": 90, "mesp": 30, "dtvt": 3600}}


def test_microtrip_keeps_only_listed_fields():
    assert json.loads(message_body(record(1, message(MICROTRIP)))) == {
        "vehicleId": "vehicle-001", "ts": 1793575800100, "ty": 4, "schemaVersion": 1, "idempotencyKey": KEY_1,
        "pld": {"tid": 354, "clt": 1793575800000, "sp": 70, "em": "28"}}


def test_body_bytes_are_compact_and_ordered():
    assert message_body(record(1)) == (
        '{"vehicleId":"vehicle-001","ts":1793575800100,"ty":4,"schemaVersion":1,'
        f'"idempotencyKey":"{KEY_1}","pld":{{"tid":354,"clt":1793575800000,"sp":70,"em":"28"}}}}')


def test_unknown_fields_are_dropped():
    raw = copy.deepcopy(MICROTRIP)
    raw["firmwareNew"] = "x"
    raw["pld"]["newSensor"] = 1
    body = json.loads(message_body(record(1, json.dumps(raw))))
    assert "firmwareNew" not in body
    assert "newSensor" not in body["pld"]


def test_phone_number_never_leaves_lambda(env):
    assert env.invoke([record(1, message(TRIP))]) == failures()
    assert "01012345678" not in env.bodies()[0]
    assert b"01012345678" not in env.s3.requests[0]["Body"]


@pytest.mark.parametrize("base", [TRIP, MICROTRIP], ids=["trip", "microtrip"])
def test_missing_kept_pld_field_is_rejected(env, base):
    records = []
    for index, field in enumerate(PLD_FIELDS[base["ty"]]):
        raw = copy.deepcopy(base)
        del raw["pld"][field]
        records.append(record(index + 1, json.dumps(raw)))
    assert env.invoke(records) == failures(*[str(index + 1) for index in range(len(records))])
    assert env.calls == []


def test_null_kept_field_is_rejected(env):
    raw = copy.deepcopy(MICROTRIP)
    raw["pld"]["em"] = None
    assert env.invoke([record(1, json.dumps(raw))]) == failures("1")


@pytest.mark.parametrize("changes", [{"vehicleId": None}, {"vehicleId": ""}, {"vehicleId": 1}, {"ts": None},
                                     {"ty": None}, {"pld": None}, {"pld": [1]}])
def test_invalid_envelope_is_rejected(env, changes):
    assert env.invoke([record(1, message(**changes))]) == failures("1")
    assert env.calls == []


@pytest.mark.parametrize("ty", [2, 5, "4", 4.0, True])
def test_unsupported_type_is_skipped_not_failed(env, capsys, ty):
    assert env.invoke([record(1, message(ty=ty)), record(2)]) == failures()
    assert len(env.s3.requests) == 1
    assert len(env.bodies()) == 1
    assert '"event":"record_skipped"' in capsys.readouterr().out


# --- 전송 동작 -------------------------------------------------------------

def test_archives_exact_utf8_body_before_sending(env):
    assert env.invoke([record(1, message(vehicleId="차량-001"))]) == failures()
    request = env.s3.requests[0]
    assert request["Bucket"] == "test-processed"
    assert request["IfNoneMatch"] == "*"
    assert request["Body"] == env.bodies()[0].encode("utf-8")
    assert env.calls == ["s3", "sqs"]


def test_batches_at_most_ten_messages(env):
    assert env.invoke([record(sequence) for sequence in range(1, 24)]) == failures()
    assert [len(request["Entries"]) for request in env.sqs.requests] == [10, 10, 3]


def test_batch_limit_counts_utf8_bytes(env):
    body = message(vehicleId="가" * 50000)
    assert env.invoke([record(1, body), record(2, body), record(3, body)]) == failures()
    assert len(env.sqs.requests) == 3
    assert all(sum(len(entry["MessageBody"].encode("utf-8")) for entry in request["Entries"]) <= 262144
               for request in env.sqs.requests)


def test_partial_sqs_failure_retries_only_unconfirmed_record(env):
    env.sqs.effects = [{"Successful": [{"Id": "0"}, {"Id": "2"}]}]
    assert env.invoke([record(1), record(2), record(3)]) == failures("2")


def test_sqs_exception_retries_entire_unconfirmed_batch(env):
    env.sqs.effects = [RuntimeError("network")]
    assert env.invoke([record(1), record(2)]) == failures("1", "2")


def test_malformed_data_does_not_block_neighbors(env):
    bad_base64 = record(2)
    bad_base64["kinesis"]["data"] = "%%%"
    assert env.invoke([record(1), bad_base64, record(3, "{broken"), record(4)]) == failures("2", "3")
    assert len(env.bodies()) == 2


def test_rejects_arrays_duplicate_keys_nonfinite_trailing_json_and_invalid_utf8(env):
    valid = message()
    invalid = ["[]", valid[:-1] + ', "vehicleId": "other"}', valid.replace('"sp": 70', '"sp": NaN'),
               valid.replace('"sp": 70', '"sp": Infinity'), valid + " {}", valid.replace('"sp": 70', '"sp": 1e400')]
    records = [record(index + 1, body) for index, body in enumerate(invalid)]
    records.append(record(7, b"\xff"))
    records.append(record(8, valid.encode("utf-16")))
    assert env.invoke(records) == failures("1", "2", "3", "4", "5", "6", "7", "8")
    assert env.calls == []


def test_message_limit_applies_to_processed_body(env):
    item = record(1)
    env.environment["MAX_MESSAGE_BYTES"] = str(len(message_body(item).encode("utf-8")) - 1)
    assert env.invoke([item]) == failures("1")
    assert env.calls == []


def test_time_budget_defers_without_writes(env):
    env.context.remaining = [1000]
    assert env.invoke([record(1), record(2)]) == failures("1", "2")
    assert env.calls == []


def test_time_budget_after_archive_defers_sqs_send(env):
    env.context.remaining = [30000, 1000]
    assert env.invoke([record(1)]) == failures("1")
    assert env.calls == ["s3"]


def test_identity_is_stable_and_distinct_across_records():
    first = message_body(record(1))
    assert first == message_body(record(1))
    key = json.loads(first)["idempotencyKey"]
    assert key != json.loads(message_body(record(2)))["idempotencyKey"]
    assert key == KEY_1


def test_missing_sqs_success_is_failure(env):
    env.sqs.effects = [{}]
    assert env.invoke([record(1)]) == failures("1")


def test_missing_sequence_fails_whole_invocation_before_writes(env):
    item = record(2)
    del item["kinesis"]["sequenceNumber"]
    with pytest.raises(ValueError):
        env.invoke([record(1), item])
    assert env.calls == []


def test_empty_batch_makes_no_aws_calls(env):
    assert env.invoke([]) == failures()
    assert env.calls == []


def test_archive_failure_does_not_send_failed_record(env):
    env.s3.effects = [RuntimeError("storage")]
    assert env.invoke([record(1), record(2)]) == failures("1")
    assert len(env.bodies()) == 1


def test_existing_archive_allows_retry_but_conflict_does_not(env):
    env.s3.effects = [client_error(412, "PreconditionFailed"), client_error(409, "ConditionalRequestConflict")]
    assert env.invoke([record(1)]) == failures()
    assert env.invoke([record(2)]) == failures("2")
    assert len(env.bodies()) == 1


def test_archive_key_is_stable_on_sqs_retry(env):
    env.sqs.effects = [RuntimeError("network"), {"Successful": [{"Id": "0"}]}]
    assert env.invoke([record(1)]) == failures("1")
    assert env.invoke([record(1)]) == failures()
    assert env.s3.requests[0]["Key"] == env.s3.requests[-1]["Key"]


def test_logs_do_not_expose_payload_or_sdk_exception_messages(env, capsys):
    env.s3.effects = [RuntimeError("secret-sdk-message")]
    assert env.invoke([record(1, message(vehicleId="private-car-id"))]) == failures("1")
    output = capsys.readouterr().out
    assert "private-car-id" not in output
    assert "secret-sdk-message" not in output
