# Python 컨테이너 Lambda

Kinesis로 들어온 차량 단말 메시지에서 점수 계산에 필요한 필드만 남겨 S3에 보관하고 SQS로 전달하는 Python 3.13 Lambda입니다.
S3 저장, SQS 전송, 재시도 규칙은 `lambda-java`와 같고, 필드 선별은 이 버전에만 있습니다.

## 실행 계약

- 핸들러: `preprocess.handler` (`src/preprocess.py`).
- 입력: AWS Kinesis 이벤트의 `Records` 배열. 각 `kinesis.data`는 Base64 JSON 객체입니다.
- 환경 변수: `QUEUE_URL`, `PROCESSED_BUCKET`, `MAX_MESSAGE_BYTES`.
- 출력: `{"batchItemFailures":[{"itemIdentifier":"실패 sequenceNumber"}]}`.
- 한 레코드당 SQS 메시지 1개, API 요청당 최대 10개. 본문과 API 배치는 256KiB 이하입니다.

## 메시지 변환

원본은 `{"vehicleId", "ts", "ty", "pld": {...}}`이고, 결과는 아래 형식입니다. `pld`에는 표의 필드만 남기고 나머지(제외 필드, 처음 보는 필드)는 버립니다.

```json
{"vehicleId": "vehicle-001", "ts": 1793575800100, "ty": 4, "schemaVersion": 1,
 "idempotencyKey": "SHA-256(stream ARN + ':' + eventID)", "pld": {"tid": 354, "clt": 1793575800000, "sp": 70, "em": "28"}}
```

| `ty` | 종류 | 남기는 `pld` 필드 |
|---|---|---|
| 3 | Trip (시동 OFF 때 1회) | `tid`, `stt`, `edt`, `dis`, `hsts`, `mesp`, `dtvt` |
| 4 | Microtrip (주행 중 주기적) | `tid`, `clt`, `sp`, `em` |

- 남길 필드 목록은 `PLD_FIELDS` 한 곳에서 관리합니다. 필드를 되살리려면(예: 공회전 점수용 `idt`) 여기에 추가합니다.
- `vehicleId`, `ts`, `ty`, `pld` 또는 남길 필드가 없거나 `null`이면 거부(실패 → 재시도)합니다.
- `ty`가 3·4가 아니면 `record_skipped` 로그만 남기고 성공으로 처리합니다. S3와 SQS에는 쓰지 않습니다.
- 값의 단위·범위는 검사하지 않습니다.

처리 순서는 `JSON 검사 → 필드 선별 → S3 저장 → SQS 전송`입니다.

- JSON 검사: 엄격한 UTF-8, 최상위 객체만 허용하고 중복 키, `NaN`/`Infinity`·범위 초과 실수, 뒤에 붙은 토큰은 거부합니다.
- S3: `processed/{key 앞 2자}/{idempotencyKey}/{본문 SHA-256}.json`에 `If-None-Match: *`로 저장합니다. 이미 있으면(412) 성공으로 봅니다. 실패한 레코드는 SQS로 보내지 않습니다.
- SQS: 응답의 `Successful`에 있는 항목만 성공입니다. 예외가 나면 해당 배치 전체를 재시도 대상으로 돌려줍니다.
- 남은 실행 시간이 12초 미만이면 새 쓰기를 하지 않고 해당 레코드를 실패로 돌려 재전달받습니다.
- 로그는 JSON 한 줄이며, payload나 SDK 예외 메시지 없이 예외 클래스 이름만 남깁니다.

## Java 버전과의 차이

- 출력 형식이 다릅니다. Java는 원본 전체를 `payload`에 넣고 Kinesis 메타데이터(`source`)를 붙이며, Python은 필드를 선별해 평탄화하고 `source`를 넣지 않습니다. `idempotencyKey` 계산 방식은 같습니다.
- boto3에는 Java SDK의 전체 호출 시간 제한(`apiCallTimeout`)이 없습니다. 대신 연결 2초, 읽기 3초, 최대 2회 시도로 제한합니다.

## 로컬 검증

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements-dev.txt   # macOS/Linux: .venv/bin/python
.venv/Scripts/python -m pytest -q
```

테스트는 S3/SQS를 가짜 객체로 대체하며 실제 AWS를 호출하지 않습니다.

## 이미지 빌드

Dockerfile은 테스트 단계를 통과해야 최종 이미지가 만들어집니다. Lambda 아키텍처에 맞춰 `linux/arm64`로 빌드합니다.

```bash
docker build --platform linux/arm64 --provenance=false -t ubi-preprocess-python .
```

ECR push와 Terraform 배포 절차는 `lambda-java/README.md`와 같습니다. 단, 기존 `scripts/publish-processing-image.sh`는 Java 이미지를 기준으로 하므로 Python 이미지를 배포하려면 빌드 경로를 바꿔야 합니다.
