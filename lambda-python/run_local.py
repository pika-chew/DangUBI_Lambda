"""Run the handler against fake S3/SQS and print every JSON it produces.

Usage: .venv/Scripts/python run_local.py ['{"ty":4,"pld":{...}}' ...]
"""
import base64
import json
import sys

sys.path.insert(0, "src")
from preprocess import PreprocessHandler  # noqa: E402

SAMPLES = [
    # Trip 원본 (제외 필드 포함)
    '{"vehicleId":"vehicle-001","ts":1793577600100,"ty":3,"pld":{"tid":354,"stt":1793574000000,'
    '"edt":1793577600000,"dis":30000,"tdis":123456789,"fc":2100,"stlat":37.509141,"stlon":127.063228,'
    '"edlat":37.510296,"edlon":127.062512,"ctp":88,"coe":5000,"fct":120,"hsts":90,"mesp":30,"idt":300,'
    '"btv":12.6,"gnv":14.2,"wut":60,"usm":"01012345678","est":600,"fwv":"1.2.3","dtvt":3600}}',
    # Microtrip 원본 (제외 필드 포함)
    '{"vehicleId":"vehicle-001","ts":1793575800100,"ty":4,"pld":{"tid":354,"fc":12,"lat":37.509141,'
    '"lon":127.063228,"lc":80,"clt":1793575800000,"cdit":15000,"rpm":2200,"sp":70,"em":"28","el":40,'
    '"xyz":{"ax":0.1,"ay":0.2,"az":9.8},"vv":12.4,"tpos":20}}',
    # 대상이 아닌 ty → 건너뜀
    '{"vehicleId":"vehicle-001","ts":1793575800100,"ty":2,"pld":[{"lat":37.5}]}',
    # 필수 필드(sp) 누락 → 거부
    '{"vehicleId":"vehicle-001","ts":1793575800100,"ty":4,"pld":{"tid":354,"clt":1793575800000,"em":"28"}}',
    # 깨진 JSON → 거부
    "{broken",
]


class FakeS3:
    def put_object(self, **kwargs):
        print(f"\n[S3 put] s3://{kwargs['Bucket']}/{kwargs['Key']}")
        return {}


class FakeSqs:
    def send_message_batch(self, **kwargs):
        for entry in kwargs["Entries"]:
            print(f"\n[SQS body #{entry['Id']}]")
            print(json.dumps(json.loads(entry["MessageBody"]), ensure_ascii=False, indent=2))
        return {"Successful": [{"Id": entry["Id"]} for entry in kwargs["Entries"]]}


class FakeContext:
    def get_remaining_time_in_millis(self):
        return 30000


def record(sequence, body):
    return {"eventSourceARN": "arn:aws:kinesis:ap-northeast-2:000000000000:stream/test",
            "eventID": f"shardId-000000000000:{sequence}",
            "kinesis": {"sequenceNumber": str(sequence), "partitionKey": "vehicle-001",
                        "approximateArrivalTimestamp": 1793240000,
                        "data": base64.b64encode(body.encode("utf-8")).decode("ascii")}}


if __name__ == "__main__":
    bodies = sys.argv[1:] or SAMPLES
    environment = {"QUEUE_URL": "https://example.test/queue",
                   "PROCESSED_BUCKET": "test-processed", "MAX_MESSAGE_BYTES": "262144"}
    handler = PreprocessHandler(FakeS3(), FakeSqs(), environment)
    print("[logs]")
    result = handler.handle_request({"Records": [record(index + 1, body) for index, body in enumerate(bodies)]},
                                    FakeContext())
    print("\n[return]")
    print(json.dumps(result, indent=2))
