"""Run the handler against a fake Firehose and print every JSON it produces.

Usage: .venv/Scripts/python run_local.py ['{"ty":4,"pld":{...}}' ...]
"""
import json
import sys

sys.path.insert(0, "src")
from preprocess import PreprocessHandler, RecordFailed  # noqa: E402

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
]


class FakeFirehose:
    def put_record(self, **kwargs):
        print(f"\n[Firehose put] {kwargs['DeliveryStreamName']}")
        print(json.dumps(json.loads(kwargs["Record"]["Data"]), ensure_ascii=False, indent=2))
        return {"RecordId": "local"}


if __name__ == "__main__":
    bodies = sys.argv[1:] or SAMPLES
    handler = PreprocessHandler(FakeFirehose(), {"FIREHOSE_STREAM_NAME": "test-delivery"})
    for body in bodies:
        print("\n--- event ---")
        try:
            print(f"[return] {handler.handle_request(json.loads(body))}")
        except RecordFailed as exception:
            print(f"[failed] {exception}")
