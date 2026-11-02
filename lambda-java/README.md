# Java 컨테이너 Lambda

이 프로젝트는 HTTP Spring 서버가 아닌 Kinesis 이벤트 처리용 Java 21 Lambda입니다.
Spring 서비스와 같은 Java 언어를 사용하지만 별도의 웹 서버를 시작하지 않습니다.
기존 서버 이미지를 그대로 재사용하려면 Lambda 런타임 호출과 Kinesis 배치 입력을 처리하는 진입점이 별도로 필요합니다.

## 책임 분담

- Java 코드: JSON 검사, 전달 메타데이터 생성, 가공 S3 저장, SQS 전송, 부분 실패 응답.
- Terraform: ECR 저장소·Lambda 이미지 읽기 정책, Lambda 이미지 URI, 환경 변수, IAM, Kinesis 연결, S3/SQS 리소스.
- 백엔드 빌드/CI: 코드 테스트, 이미지 빌드, ECR push, 배포할 이미지 digest 전달.

Terraform이 ECR 저장소와 Lambda의 이미지 읽기 정책을 생성합니다. 이미지 빌드·push는 `scripts/publish-processing-image.sh` 또는 백엔드 CI가 담당합니다.
다음 이미지는 프로젝트 계정·서울 리전의 private ECR에 있어야 하며 Lambda 아키텍처와 같은 `linux/arm64` 단일 아키텍처여야 합니다.

## 실행 계약

- 핸들러: `com.ubi.processing.PreprocessHandler::handleRequest`.
- 입력: AWS Kinesis 이벤트의 `Records` 배열. 각 `kinesis.data`는 Base64 JSON 객체입니다.
- 환경 변수: `QUEUE_URL`, `PROCESSED_BUCKET`, `MAX_MESSAGE_BYTES`.
- 출력: `{"batchItemFailures":[{"itemIdentifier":"실패 sequenceNumber"}]}`.
- 한 레코드당 SQS 메시지 1개, API 요청당 최대 10개. 본문과 API 배치는 보수적으로 256KiB 이하입니다.
- `schemaVersion=1`, `idempotencyKey=SHA-256(stream ARN + ':' + eventID)`로 기존 전송 식별 방식을 유지합니다.
- 현재 SQS 본문은 `schemaVersion`, `idempotencyKey`, `source`, `payload` 구조입니다. `vehicleId`, `ty`, `ts`, `pld`는 원문 `payload`에 보존됩니다. 최상위 평탄화, 도메인 필드 선별·단위·범위 검증은 아직 구현하지 않았습니다.

처리 순서는 `JSON 검사 → 본문 생성 → S3 저장 → SQS 전송`입니다.
S3와 SQS에는 같은 UTF-8 본문을 사용합니다. S3의 조건부 쓰기는 같은 객체가 이미 있으면 재사용합니다.
S3 실패 시 해당 레코드는 SQS로 보내지 않습니다. SQS 실패 시 재시도하며, S3에 먼저 저장된 객체는 남습니다.
객체 존재만으로 SQS 전송 성공을 판정할 수 없습니다. 프라이빗의 중복 반영 방지는 별도로 필요합니다.

## 로컬 검증

JDK 21 이상과 Gradle 9.8.2를 사용합니다. 컴파일 결과는 Java 21 대상으로 고정합니다.

```bash
gradle --no-daemon test stage
```

테스트는 AWS SDK 클라이언트를 대체하며 실제 S3/SQS를 호출하지 않습니다.
테스트 결과는 `build/reports/tests/test/index.html`, 배포 파일은 `build/lambda/lib/`에 생성됩니다.

## 이미지 빌드와 전달

첫 배포에서는 이미지 업로드 전에 빈 ECR 저장소가 필요하므로 저장소만 먼저 생성합니다. 아래 명령은 Terraform 루트 디렉토리에서 실행합니다.
`-target`은 초기 저장소 부트스트랩에만 사용하며, 이미지 준비 후에는 전체 계획을 검토합니다.
AWS 실행 사용자에게 ECR 관리·push 권한이 없다면 관리자가 `permissions/stage5-ecr-policy.json`을 기존 권한에 추가해야 합니다. 로컬 정책 파일만으로 AWS 권한이 바뀌지는 않습니다.

```bash
export AWS_PROFILE=pika-ubi
terraform init
terraform plan -input=false \
  -target=module.processing.aws_ecr_repository.preprocess \
  -target=module.processing.aws_ecr_repository_policy.preprocess \
  -out=ecr-bootstrap.tfplan
terraform apply ecr-bootstrap.tfplan
```

계획에는 ECR 저장소와 정책만 추가되어야 합니다. 다른 리소스 변경·삭제가 있다면 먼저 원인을 확인합니다.
`processing_image_uri`가 null인 상태의 전체 배포는 Lambda 사전조건에서 중단되므로, 첫 단계에서는 기존 함수가 교체되지 않습니다.

Docker Desktop을 실행한 뒤 루트에서 다음 명령으로 이미지 빌드·업로드를 수행합니다.

```bash
bash scripts/publish-processing-image.sh
```

스크립트는 ECR 로그인 → Java 테스트를 포함한 `linux/arm64` 이미지 빌드·push → 실제 digest 조회 → `processing-image.auto.tfvars.json` 저장을 수행합니다.
Dockerfile은 Lambda Java 21 기본 이미지를 사용하므로 런타임 인터페이스가 포함되어 있습니다. 업로드 실패 시 이미지 주소를 갱신하지 않습니다.
파일은 Git에서 제외되며 Terraform이 자동으로 읽습니다. 다른 tfvars나 `-var`에서 같은 변수를 지정하고 있다면 충돌 여부를 확인합니다.
태그는 변경 불가로 설정되어 있어 매번 고유한 태그를 사용합니다. 기본 태그는 UTC 시각을 포함하며 원하는 태그는 스크립트의 첫 인자로 전달할 수 있습니다.

이미지 업로드가 끝난 뒤 전체 계획을 새로 생성합니다.

```bash
terraform plan -input=false -out=review.tfplan
terraform apply review.tfplan
```

첫 Image 전환의 함수 교체 범위를 확인한 뒤 적용합니다. 스크립트는 Lambda 배포나 Terraform apply를 실행하지 않습니다.
이미지 digest로 배포를 고정하므로 같은 태그를 재사용해 변경이 감지되지 않는 문제를 피합니다.
ECR은 `force_delete=false`이며 이미지 자동 삭제 정책은 아직 없습니다. 롤백에 필요한 이미지를 임의로 만료시키지 않습니다.

CI에서 별도 빌드하는 경우에는 이 저장소의 `linux/arm64` Lambda 호환 이미지를 push한 뒤 다음 형식의 URI를 `processing_image_uri`에 전달하면 됩니다.

```text
<account>.dkr.ecr.ap-northeast-2.amazonaws.com/ubi-dev-preprocess@sha256:<64자리 digest>
```

Terraform이 Lambda 서비스의 `ecr:BatchGetImage`, `ecr:GetDownloadUrlForLayer`를 허용하는 저장소 정책을 관리합니다.
이미지 push·저장소 정책 변경 권한과 Lambda 실행 역할의 S3/SQS 권한은 서로 다른 권한입니다.

## 기존 ZIP 함수에서 전환

AWS는 같은 함수의 ZIP/Image 배포 형식 변경을 지원하지 않습니다. 현재 Terraform 주소·함수 이름을 유지한 채 형식을 변경하므로 **기존 Lambda가 삭제 후 재생성되는 계획**이 예상됩니다.
실제 이미지가 준비된 뒤 새 계획에서 교체 여부와 Kinesis 이벤트 소스 매핑 영향을 반드시 확인합니다. 이전 Python 배포용 계획은 적용하지 않습니다.
교체 동안 처리가 지연되거나 재전달될 수 있습니다. 무중단 전환이 필요하면 다른 이름의 Image Lambda를 먼저 만들고 소비자를 전환하는 별도 마이그레이션이 필요합니다.
이번 변경은 로컬 코드 준비이며 AWS 배포·이미지 push는 수행하지 않습니다.

## 공식 문서

- [Java Lambda 컨테이너 이미지](https://docs.aws.amazon.com/lambda/latest/dg/java-image.html)
- [이미지 요구사항과 ZIP/Image 전환 제한](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html)
