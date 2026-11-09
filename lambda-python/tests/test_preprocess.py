import copy
import hashlib
import json
from pathlib import Path

import pytest

from preprocess import PLD_FIELDS, PreprocessHandler, RecordFailed, message_body

# 단말이 보내는 원본. 제외 필드까지 모두 들어 있다.
TRIP = {"vehicleId": "vehicle-001", "ts": 1793577600100, "ty": 3, "pld": {
    "tid": 354, "stt": 1793574000000, "edt": 1793577600000, "dis": 30000, "tdis": 123456789, "fc": 2100,
    "stlat": 37.509141, "stlon": 127.063228, "edlat": 37.510296, "edlon": 127.062512, "ctp": 88, "coe": 5000,
    "fct": 120, "hsts": 90, "mesp": 30, "idt": 300, "btv": 12.6, "gnv": 14.2, "wut": 60, "usm": "01012345678",
    "est": 600, "fwv": "1.2.3", "dtvt": 3600}}
MICROTRIP = {"vehicleId": "vehicle-001", "ts": 1793575800100, "ty": 4, "pld": {
    "tid": 354, "fc": 12, "lat": 37.509141, "lon": 127.063228, "lc": 80, "clt": 1793575800000, "cdit": 15000,
    "rpm": 2200, "sp": 70, "em": "28", "el": 40, "xyz": {"ax": 0.1, "ay": 0.2, "az": 9.8}, "vv": 12.4, "tpos": 20}}
# MICROTRIP의 idempotencyKey. 계산 입력이 바뀌면 받는 쪽의 중복 제거가 깨지므로 형식을 고정해 검사한다.
MICROTRIP_KEY = hashlib.sha256(
    b'{"vehicleId":"vehicle-001","ts":1793575800100,"ty":4,'
    b'"pld":{"tid":354,"clt":1793575800000,"sp":70,"em":"28"}}').hexdigest()


class FakeFirehose:
    def __init__(self):
        self.requests = []
        self.effects = []

    def put_record(self, **kwargs):
        self.requests.append(kwargs)
        if self.effects:
            raise self.effects.pop(0)
        return {"RecordId": "test"}


@pytest.fixture
def env():
    environment = {"FIREHOSE_STREAM_NAME": "test-delivery"}
    firehose = FakeFirehose()
    handler = PreprocessHandler(firehose, environment)

    class Env:
        pass

    result = Env()
    result.environment, result.firehose = environment, firehose
    result.invoke = handler.handle_request
    result.records = lambda: [request["Record"]["Data"] for request in firehose.requests]
    return result


def message(base=MICROTRIP, **changes):
    """원본 메시지를 복사해 최상위 필드를 바꾼다. 값이 None이면 그 필드를 지운다."""
    result = copy.deepcopy(base)
    for key, value in changes.items():
        if value is None:
            result.pop(key, None)
        else:
            result[key] = value
    return result


def without_key(body):
    result = json.loads(body)
    assert len(result.pop("idempotencyKey")) == 64
    return result


# --- 필드 선별 -------------------------------------------------------------

def test_trip_keeps_only_listed_fields():
    assert without_key(message_body(message(TRIP))) == {
        "vehicleId": "vehicle-001", "ts": 1793577600100, "ty": 3, "schemaVersion": 1,
        "pld": {"tid": 354, "stt": 1793574000000, "edt": 1793577600000, "dis": 30000,
                "hsts": 90, "mesp": 30, "dtvt": 3600}}


def test_microtrip_keeps_only_listed_fields():
    assert without_key(message_body(message())) == {
        "vehicleId": "vehicle-001", "ts": 1793575800100, "ty": 4, "schemaVersion": 1,
        "pld": {"tid": 354, "clt": 1793575800000, "sp": 70, "em": "28"}}


def test_body_bytes_are_compact_and_ordered():
    assert message_body(message()) == (
        '{"vehicleId":"vehicle-001","ts":1793575800100,"ty":4,"schemaVersion":1,'
        f'"idempotencyKey":"{MICROTRIP_KEY}","pld":{{"tid":354,"clt":1793575800000,"sp":70,"em":"28"}}}}')


def test_unknown_fields_are_dropped():
    raw = message()
    raw["firmwareNew"] = "x"
    raw["pld"]["newSensor"] = 1
    body = json.loads(message_body(raw))
    assert "firmwareNew" not in body
    assert "newSensor" not in body["pld"]


def test_phone_number_never_leaves_lambda(env):
    assert env.invoke(message(TRIP)) == {"status": "forwarded"}
    assert b"01012345678" not in env.records()[0]


@pytest.mark.parametrize("base", [TRIP, MICROTRIP], ids=["trip", "microtrip"])
def test_missing_kept_pld_field_is_rejected(env, base):
    for field in PLD_FIELDS[base["ty"]]:
        raw = message(base)
        del raw["pld"][field]
        with pytest.raises(RecordFailed):
            env.invoke(raw)
    assert env.firehose.requests == []


def test_null_kept_field_is_rejected(env):
    raw = message()
    raw["pld"]["em"] = None
    with pytest.raises(RecordFailed):
        env.invoke(raw)
    assert env.firehose.requests == []


@pytest.mark.parametrize("changes", [{"vehicleId": None}, {"vehicleId": ""}, {"vehicleId": 1}, {"ts": None},
                                     {"ty": None}, {"pld": None}, {"pld": [1]}])
def test_invalid_envelope_is_rejected(env, changes):
    with pytest.raises(RecordFailed):
        env.invoke(message(**changes))
    assert env.firehose.requests == []


@pytest.mark.parametrize("event", [[], "text", None, {"Records": []}], ids=["list", "string", "null", "kinesis"])
def test_event_that_is_not_a_message_is_rejected(env, event):
    with pytest.raises(RecordFailed):
        env.invoke(event)
    assert env.firehose.requests == []


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")], ids=["nan", "inf", "-inf"])
def test_non_finite_number_is_rejected(env, value):
    raw = message()
    raw["pld"]["sp"] = value
    with pytest.raises(RecordFailed):
        env.invoke(raw)
    assert env.firehose.requests == []


@pytest.mark.parametrize("ty", [2, 5, "4", 4.0, True])
def test_unsupported_type_is_skipped_not_failed(env, capsys, ty):
    assert env.invoke(message(ty=ty)) == {"status": "skipped"}
    assert env.firehose.requests == []
    assert '"event":"record_skipped"' in capsys.readouterr().out


def test_smoke_event_is_forwarded(env):
    # Jenkins가 CI Lambda에 보내는 이벤트. Jenkinsfile이 이 응답을 검사하므로 CI Firehose까지 전달돼야 한다.
    event = json.loads((Path(__file__).parent / "smoke-event.json").read_text(encoding="utf-8"))
    assert env.invoke(event) == {"status": "forwarded"}
    assert len(env.records()) == 1


# --- 전송 동작 -------------------------------------------------------------

def test_sends_one_newline_terminated_utf8_record(env):
    assert env.invoke(message(vehicleId="차량-001")) == {"status": "forwarded"}
    assert len(env.firehose.requests) == 1
    assert env.firehose.requests[0]["DeliveryStreamName"] == "test-delivery"
    data = env.records()[0]
    assert data.endswith(b"\n") and data.count(b"\n") == 1
    assert "차량-001".encode("utf-8") in data


def test_firehose_failure_fails_invocation_for_retry(env):
    env.firehose.effects = [RuntimeError("network")]
    with pytest.raises(RecordFailed):
        env.invoke(message())


def test_retry_produces_same_record(env):
    env.firehose.effects = [RuntimeError("network")]
    with pytest.raises(RecordFailed):
        env.invoke(message())
    assert env.invoke(message()) == {"status": "forwarded"}
    assert env.records()[0] == env.records()[1]


def test_identity_is_distinct_across_messages():
    keys = {json.loads(message_body(item))["idempotencyKey"]
            for item in [message(), message(ts=1793575800101), message(vehicleId="vehicle-002"), message(TRIP)]}
    assert len(keys) == 4


def test_identity_ignores_dropped_fields():
    raw = message()
    raw["pld"]["rpm"] = 9999
    assert json.loads(message_body(raw))["idempotencyKey"] == MICROTRIP_KEY


@pytest.mark.parametrize("value", [None, "", "  "], ids=["missing", "empty", "blank"])
def test_missing_setting_fails_before_writes(env, value):
    env.environment.pop("FIREHOSE_STREAM_NAME")
    if value is not None:
        env.environment["FIREHOSE_STREAM_NAME"] = value
    with pytest.raises(RuntimeError):
        env.invoke(message())
    assert env.firehose.requests == []


# --- 로그 -------------------------------------------------------------------

def summary(output):
    lines = [json.loads(line) for line in output.splitlines()]
    return next(line for line in lines if line["event"] == "processing_summary")


def test_summary_reports_each_outcome_for_failure_metric(env, capsys):
    env.invoke(message())
    assert summary(capsys.readouterr().out) == {"received": 1, "skipped": 0, "failed": 0,
                                                "event": "processing_summary"}
    env.invoke(message(ty=2))
    assert summary(capsys.readouterr().out)["skipped"] == 1
    env.firehose.effects = [RuntimeError("network")]
    with pytest.raises(RecordFailed):
        env.invoke(message())
    assert summary(capsys.readouterr().out)["failed"] == 1
    with pytest.raises(RecordFailed):
        env.invoke(message(vehicleId=None))
    assert summary(capsys.readouterr().out)["failed"] == 1


def test_logs_and_exception_do_not_expose_payload_or_sdk_messages(env, capsys):
    env.firehose.effects = [RuntimeError("secret-sdk-message")]
    with pytest.raises(RecordFailed) as caught:
        env.invoke(message(vehicleId="private-car-id"))
    output = capsys.readouterr().out
    assert "private-car-id" not in output
    assert "secret-sdk-message" not in output
    assert "secret-sdk-message" not in str(caught.value)
    assert caught.value.__suppress_context__
