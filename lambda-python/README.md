# Python 컨테이너 Lambda

IoT Core로 들어온 차량 단말 메시지에서 점수 계산에 필요한 필드만 남겨 Firehose로 전달하는 Python 3.13 Lambda입니다.

전체 흐름은 `IoT Core → Lambda → Firehose → S3 → SQS`입니다. S3 저장과 SQS 알림은 Firehose와 S3 이벤트 알림이 맡습니다.

## 실행 계약

- 핸들러: `preprocess.handler` (`src/preprocess.py`).
- 입력: IoT Rule이 호출할 때 넘기는 메시지 JSON 객체 하나 (`SELECT *, topic(4) AS vehicleId ...`의 결과).
- 환경 변수: `FIREHOSE_STREAM_NAME` (Firehose 스트림 이름).
- 권한: 대상 스트림의 `firehose:PutRecord`.
- 출력: `{"status":"forwarded"}` 또는 `{"status":"skipped"}`. 실패하면 예외를 던집니다.
- 메시지 하나당 Firehose 레코드 1개. 레코드는 줄바꿈으로 끝나므로 S3 객체는 한 줄에 JSON 하나인 형식(NDJSON)입니다.

## 메시지 변환

원본은 `{"vehicleId", "ts", "ty", "pld": {...}}`이고, 결과는 아래 형식입니다. `pld`에는 표의 필드만 남기고 나머지(제외 필드, 처음 보는 필드)는 버립니다.

```json
{"vehicleId": "vehicle-001", "ts": 1793575800100, "ty": 4, "schemaVersion": 1,
 "idempotencyKey": "SHA-256(vehicleId, ts, ty, 선별된 pld의 JSON)", "pld": {"tid": 354, "clt": 1793575800000, "sp": 70, "em": "28"}}
```

| `ty` | 종류 | 남기는 `pld` 필드 |
|---|---|---|
| 3 | Trip (시동 OFF 때 1회) | `tid`, `stt`, `edt`, `dis`, `hsts`, `mesp`, `dtvt` |
| 4 | Microtrip (주행 중 주기적) | `tid`, `clt`, `sp`, `em` |

- 남길 필드 목록은 `PLD_FIELDS` 한 곳에서 관리합니다. 필드를 되살리려면(예: 공회전 점수용 `idt`) 여기에 추가합니다.
- `vehicleId`, `ts`, `ty`, `pld` 또는 남길 필드가 없거나 `null`이면 거부합니다.
- `ty`가 3·4가 아니면 `record_skipped` 로그만 남기고 성공으로 처리합니다. Firehose에는 쓰지 않습니다.
- 값의 단위·범위는 검사하지 않습니다. `NaN`/`Infinity`는 거부합니다.

처리 순서는 `검증 → 필드 선별 → Firehose 전송`입니다.

- 실패: 거부나 전송 실패는 호출을 실패시킵니다. IoT Rule의 비동기 호출이므로 Lambda가 재시도하고, 그래도 실패한 이벤트는 Lambda에 설정한 실패 목적지로 갑니다.
- 중복: Firehose와 비동기 호출은 최소 1회 전달이라 같은 레코드가 두 번 저장될 수 있습니다. 받는 쪽은 `idempotencyKey`로 걸러야 합니다.
- 로그는 JSON 한 줄이며, payload나 SDK 예외 메시지 없이 예외 클래스 이름만 남깁니다. `processing_summary`의 `failed`는 CloudWatch 지표 `ProcessingFailedRecords`가 읽습니다.
- boto3 호출은 연결 2초, 읽기 3초, 최대 2회 시도로 제한합니다.

`lambda-java`는 이전 흐름(Kinesis → S3·SQS 직접 쓰기) 그대로이며 배포 대상이 아닙니다.

## 로컬 검증

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements-dev.txt   # macOS/Linux: .venv/bin/python
.venv/Scripts/python -m pytest -q
```

테스트는 Firehose를 가짜 객체로 대체하며 실제 AWS를 호출하지 않습니다.

## 이미지 빌드

Dockerfile은 테스트 단계를 통과해야 최종 이미지가 만들어집니다. Lambda 아키텍처에 맞춰 `linux/arm64`로 빌드합니다.

```bash
docker build --platform linux/arm64 --provenance=false -t ubi-preprocess-python .
```

ECR push와 Terraform 배포 절차는 `lambda-java/README.md`와 같습니다. 단, 기존 `scripts/publish-processing-image.sh`는 Java 이미지를 기준으로 하므로 Python 이미지를 배포하려면 빌드 경로를 바꿔야 합니다.
