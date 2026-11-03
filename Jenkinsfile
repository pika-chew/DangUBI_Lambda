// 전처리 Lambda(lambda-python) CI/CD 파이프라인 (Multibranch Pipeline + GitHub Branch Source)
//  - PR·브랜치 빌드 : 시크릿 검사 → 단위 테스트 → 의존성 취약점 검사
//  - main 빌드      : 위 단계 + 사전 점검 → arm64 이미지 빌드·스캔 → ECR push → Lambda 교체 → 스모크 테스트(실패 시 롤백)
//                     → IaC 레포의 이미지 digest 동기화
//  - lambda-python/이 바뀌지 않았으면(Jenkinsfile·문서만 변경) Test·SCA·배포를 건너뛰고 Secret Scan만 한다.
//    비교 기준: PR은 대상 브랜치, 그 밖은 마지막 성공 빌드. FORCE_DEPLOY를 켜면 변경과 상관없이 전부 실행한다.
//
// Jenkins에는 docker CLI만 있으므로, 도구는 단계마다 공식 컨테이너 이미지로 실행한다.
// (docker.image().inside는 Jenkins 컨테이너의 볼륨을 그대로 공유하므로 워크스페이스 경로가 같다)
//
// 사전 준비
//  - Jenkins Credentials: AWS_CRED_ID (Username with password 타입, username=Access Key ID, password=Secret Access Key)
//    필요 권한: sts:GetCallerIdentity, ecr:GetAuthorizationToken, ECR 저장소 push,
//              lambda:GetFunction, lambda:GetFunctionConfiguration, lambda:UpdateFunctionCode, lambda:InvokeFunction
//  - Jenkins Credentials: IAC_CRED_ID (Username with password 타입, password=DangUBI_Infra 쓰기 권한 GitHub 토큰)
//  - Jenkins 호스트: arm64 에뮬레이션(binfmt) 1회 설치  docker run --privileged --rm tonistiigi/binfmt --install arm64
//  - Terraform: Lambda는 Image 타입·arm64여야 한다 (terraform_aws/modules/processing/lambda.tf).
//    배포는 Jenkins가 직접 하고, IaC 레포의 IAC_IMAGE_FILE에는 배포된 digest를 기록만 해서 코드와 실제 상태를 맞춘다.
//  - Slack: Manage Jenkins → System → Slack에 워크스페이스와 Bot 토큰(xoxb-)을 등록하고, 봇을 SLACK_CHANNEL에 초대한다.
//
// Slack 알림 (빌드가 끝났을 때 결과만 한 번 보낸다)
//  - main : 모든 결과 — 성공(배포함/배포 대상 변경 없음), 롤백, IaC 동기화 실패, 실패, 중단
//  - PR   : 실패, 정상화(fixed)
//  - 그 밖의 브랜치: 없음

pipeline {
    agent any

    parameters {
        booleanParam(name: 'FORCE_DEPLOY', defaultValue: false, description: 'lambda-python/ 변경이 없어도 테스트를 돌리고, main이면 이미지를 새로 빌드해 배포')
    }

    options {
        disableConcurrentBuilds()
        timestamps()
        timeout(time: 30, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '30'))
    }

    environment {
        AWS_REGION         = 'ap-northeast-2'
        AWS_DEFAULT_REGION = 'ap-northeast-2'
        AWS_CRED_ID        = 'aws-ecr-jenkins'          // Jenkins Credentials ID
        FUNCTION_NAME      = 'ubi-dev-preprocess'     // Terraform: "${project_name}-${environment}-preprocess"
        ECR_REPOSITORY     = 'ubi-dev-preprocess'
        APP_DIR            = 'lambda-python'

        // 배포 후 digest를 기록할 IaC 레포 (main에 직접 push)
        IAC_REPO_URL       = 'https://github.com/pika-chew/DangUBI_Infra.git'
        IAC_BRANCH         = 'main'
        IAC_IMAGE_FILE     = 'terraform_aws/processing-image.auto.tfvars.json'
        IAC_CRED_ID        = 'github-infra-push'      // Jenkins Credentials ID

        SLACK_CHANNEL      = '#dangubi-deploy'        // TODO: 알림 채널
        PLATFORM           = 'linux/arm64'            // Lambda architectures와 같아야 함
        REPORT_DIR         = 'reports'

        // 도구 이미지 (버전을 고정해 빌드를 재현 가능하게 유지)
        PYTHON_IMAGE   = 'python:3.13-slim'
        AWS_CLI_IMAGE  = 'amazon/aws-cli:2.17.0'
        DOCKER_IMAGE   = 'docker:27.3.1-cli'
        TRIVY_IMAGE    = 'aquasec/trivy:0.58.1'
        GITLEAKS_IMAGE = 'zricethezav/gitleaks:v8.21.2'
    }

    stages {
        stage('Prepare') {
            steps {
                sh 'mkdir -p ${REPORT_DIR}'
                script {
                    env.COMMIT_SUBJECT = sh(script: 'git log -1 --format=%s', returnStdout: true).trim()
                    env.COMMIT_AUTHOR = sh(script: 'git log -1 --format=%an', returnStdout: true).trim()
                    env.CODE_CHANGED = (params.FORCE_DEPLOY || codeChanged()).toString()
                    env.DEPLOY = (env.BRANCH_NAME == 'main' && env.CODE_CHANGED == 'true').toString()
                    echo "${APP_DIR} 변경: ${env.CODE_CHANGED == 'true' ? '있음 → 테스트 실행' : '없음 → Test·SCA·배포 생략'}"
                    echo "배포 대상: ${env.DEPLOY == 'true' ? FUNCTION_NAME : '없음'}"
                }
            }
        }

        stage('Secret Scan') {
            steps {
                inContainer(env.GITLEAKS_IMAGE) {
                    sh 'gitleaks detect --source . --no-banner --redact --report-path ${REPORT_DIR}/gitleaks.json'
                }
            }
        }

        stage('Test') {
            when { environment name: 'CODE_CHANGED', value: 'true' }
            steps {
                inContainer(env.PYTHON_IMAGE) {
                    dir(env.APP_DIR) {
                        sh '''
                            python -m venv .venv
                            .venv/bin/pip install -q --no-cache-dir -r requirements-dev.txt
                            .venv/bin/python -m pytest -q --junitxml=../${REPORT_DIR}/test-results.xml
                        '''
                    }
                }
            }
        }

        stage('SCA') {
            when { environment name: 'CODE_CHANGED', value: 'true' }
            steps {
                inContainer(env.TRIVY_IMAGE) {
                    sh 'trivy fs --cache-dir .trivy-cache --scanners vuln --severity HIGH,CRITICAL --exit-code 1 --skip-dirs "**/.venv" ${APP_DIR}'
                }
            }
        }

        // ───────────────────── main 배포 ─────────────────────
        stage('Deploy') {
            when { environment name: 'DEPLOY', value: 'true' }
            stages {
                // 배포 대상이 Image 타입·arm64인지, 이 호스트가 arm64 이미지를 만들 수 있는지 먼저 확인
                stage('Preflight') {
                    steps {
                        withAws {
                            script {
                                def account = sh(script: 'aws sts get-caller-identity --query Account --output text', returnStdout: true).trim()
                                def packageType = lambdaQuery('PackageType')
                                def architecture = lambdaQuery('Architectures[0]')
                                if (packageType != 'Image' || architecture != 'arm64') {
                                    error("${FUNCTION_NAME}은(는) ${packageType}/${architecture}입니다. Terraform에서 Image/arm64로 먼저 전환해야 합니다.")
                                }
                                env.PREVIOUS_IMAGE = sh(script: 'aws lambda get-function --function-name "$FUNCTION_NAME" --query Code.ResolvedImageUri --output text',
                                                        returnStdout: true).trim()
                                env.ECR_REGISTRY = "${account}.dkr.ecr.${AWS_REGION}.amazonaws.com"
                                // ECR 태그는 변경 불가이므로 같은 커밋을 다시 빌드해도 겹치지 않게 빌드 번호를 붙인다
                                env.IMAGE = "${env.ECR_REGISTRY}/${ECR_REPOSITORY}:${env.GIT_COMMIT.take(7)}-b${env.BUILD_NUMBER}"
                            }
                        }
                        withDocker {
                            script {
                                def arch = sh(script: 'docker run --rm --platform "$PLATFORM" "$PYTHON_IMAGE" uname -m', returnStdout: true).trim()
                                if (arch != 'aarch64') {
                                    error("이 Jenkins 호스트는 arm64 이미지를 실행할 수 없습니다. 호스트에서 binfmt를 설치하세요: docker run --privileged --rm tonistiigi/binfmt --install arm64")
                                }
                            }
                        }
                    }
                }

                stage('Image Build & Scan') {
                    steps {
                        withDocker {
                            // --provenance=false: Lambda는 attestation이 들어간 이미지 인덱스를 받지 않는다
                            sh 'docker buildx build --platform "$PLATFORM" --provenance=false --load -t "$IMAGE" "$APP_DIR"'
                        }
                        inContainer(env.TRIVY_IMAGE, dockerSocketArgs()) {
                            sh 'trivy image --cache-dir .trivy-cache --severity CRITICAL --ignore-unfixed --exit-code 1 "$IMAGE"'
                        }
                    }
                }

                stage('Push to ECR') {
                    steps {
                        // 로그인 정보는 워크스페이스 안의 DOCKER_CONFIG에 두고, 빌드가 끝나면 cleanWs로 지운다
                        withEnv(["DOCKER_CONFIG=${env.WORKSPACE}/.docker"]) {
                            withAws {
                                sh 'mkdir -p "$DOCKER_CONFIG" && umask 077 && aws ecr get-login-password > "$DOCKER_CONFIG/ecr-token"'
                            }
                            withDocker {
                                sh '''
                                    docker login --username AWS --password-stdin "$ECR_REGISTRY" < "$DOCKER_CONFIG/ecr-token"
                                    rm -f "$DOCKER_CONFIG/ecr-token"
                                    docker push "$IMAGE"
                                '''
                                script {
                                    // 태그 대신 digest로 배포해야 무엇이 배포됐는지 정확히 남는다
                                    env.IMAGE_DIGEST_URI = sh(script: 'docker inspect --format "{{index .RepoDigests 0}}" "$IMAGE"', returnStdout: true).trim()
                                    currentBuild.description = env.IMAGE_DIGEST_URI
                                }
                            }
                        }
                    }
                }

                stage('Deploy & Smoke Test') {
                    steps {
                        withAws {
                            script { env.LAMBDA_UPDATED = 'true' }  // 이후 중단되면 검증되지 않은 이미지가 남을 수 있다
                            sh '''
                                aws lambda update-function-code --function-name "$FUNCTION_NAME" --image-uri "$IMAGE_DIGEST_URI" > /dev/null
                                aws lambda wait function-updated --function-name "$FUNCTION_NAME"
                            '''
                            echo "${FUNCTION_NAME}: ${env.PREVIOUS_IMAGE} → ${env.IMAGE_DIGEST_URI}"
                            script {
                                // ty=0 레코드 하나 → 건너뛰기 처리되어 S3/SQS에 아무것도 쓰지 않고 빈 실패 목록이 와야 한다
                                def rc = sh(returnStatus: true, script: '''
                                    aws lambda invoke --function-name "$FUNCTION_NAME" \
                                        --payload fileb://${APP_DIR}/tests/smoke-event.json \
                                        ${REPORT_DIR}/smoke-response.json > ${REPORT_DIR}/smoke-invoke.json
                                    if grep -q FunctionError ${REPORT_DIR}/smoke-invoke.json; then exit 1; fi
                                    tr -d ' \\n' < ${REPORT_DIR}/smoke-response.json | grep -qx '{"batchItemFailures":\\[\\]}'
                                ''')
                                if (rc != 0) {
                                    sh 'cat ${REPORT_DIR}/smoke-response.json || true'
                                    echo "스모크 테스트 실패 → ${env.PREVIOUS_IMAGE} 로 롤백"
                                    sh '''
                                        aws lambda update-function-code --function-name "$FUNCTION_NAME" --image-uri "$PREVIOUS_IMAGE" > /dev/null
                                        aws lambda wait function-updated --function-name "$FUNCTION_NAME"
                                    '''
                                    env.ROLLED_BACK = 'true'
                                    error("${FUNCTION_NAME} 스모크 테스트 실패 (롤백 완료)")
                                }
                            }
                        }
                    }
                }

                // 배포가 끝난 digest를 IaC 레포에 기록한다. Lambda는 이미 교체됐으므로 실패해도 배포를 되돌리지 않고 UNSTABLE로 표시한다.
                stage('Sync IaC') {
                    steps {
                        catchError(buildResult: 'UNSTABLE', stageResult: 'FAILURE') {
                            // try/catch는 Groovy 코드라 Declarative steps 안에서는 script 블록이 필요하다
                            script {
                                try {
                                    syncIacImage()
                                } catch (e) {
                                    env.IAC_SYNC_FAILED = 'true'
                                    throw e
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    post {
        always {
            archiveArtifacts artifacts: "${REPORT_DIR}/**", allowEmptyArchive: true
            sh 'if [ -n "$IMAGE" ]; then docker image rm "$IMAGE" || true; fi'
        }
        // ── main: 결과마다 한 번 ──
        success {
            script {
                if (env.BRANCH_NAME == 'main') {
                    def result = env.DEPLOY == 'true'
                        ? "배포 `${shortDigest(env.PREVIOUS_IMAGE)}` → `${shortDigest(env.IMAGE_DIGEST_URI)}`"
                        : "${APP_DIR} 변경 없음 (테스트·배포 생략)"
                    notifyMain('good', "✅ *main 성공* · ${result}")
                }
            }
        }
        unstable {
            script {
                if (env.BRANCH_NAME == 'main' && env.IAC_SYNC_FAILED == 'true') {
                    notifyMain('warning', "🟡 *main 배포 성공, IaC 동기화 실패* · `${IAC_IMAGE_FILE}`의 `processing_image_uri`를 직접 맞춰 주세요:\n`${env.IMAGE_DIGEST_URI}`")
                } else if (env.BRANCH_NAME == 'main') {
                    notifyMain('warning', "🟡 *main 불안정* · ${buildLink('console', 'Console Output')}")
                }
            }
        }
        failure {
            script {
                if (env.BRANCH_NAME == 'main' && env.ROLLED_BACK == 'true') {
                    notifyMain('danger', "<!here> 🔴 *main 배포 실패* · 스모크 테스트 실패 → 이전 이미지 `${shortDigest(env.PREVIOUS_IMAGE)}`로 롤백 완료 · " +
                        buildLink("artifact/${env.REPORT_DIR}/smoke-response.json", 'smoke-response.json'))
                } else if (env.BRANCH_NAME == 'main') {
                    notifyMain('danger', "🔴 *main 실패* · 운영 Lambda는 바뀌지 않음 · ${buildLink('console', 'Console Output')}")
                } else if (env.CHANGE_ID) {
                    slackSend(channel: env.SLACK_CHANNEL, color: 'danger', message: "🔴 ${prLine()} 검사 실패 · ${buildLink('console', 'Console Output')}")
                }
            }
        }
        aborted {
            script {
                if (env.BRANCH_NAME == 'main' && env.LAMBDA_UPDATED == 'true') {
                    // Lambda 교체 후 스모크 테스트 전에 끊기면 검증되지 않은 이미지가 운영에 남는다
                    notifyMain('#999999', "<!here> ⚪ *main 배포 중단* · Lambda가 검증되지 않은 이미지 `${shortDigest(env.IMAGE_DIGEST_URI)}`일 수 있습니다. 확인해 주세요")
                } else if (env.BRANCH_NAME == 'main') {
                    notifyMain('#999999', "⚪ *main 중단* · 운영 Lambda는 바뀌지 않음")
                }
            }
        }
        // ── PR: 실패하던 검사가 다시 통과 ──
        fixed {
            script {
                if (env.CHANGE_ID) {
                    slackSend(channel: env.SLACK_CHANNEL, color: 'good', message: "✅ ${prLine()} 검사 통과 (이전 실패 해결) · ${buildLink()}")
                }
            }
        }
        cleanup { cleanWs() }
    }
}

// ───────────────────────── helpers ─────────────────────────

// lambda-python/이 바뀌었는지. Jenkinsfile·문서 변경은 이미지 내용과 무관하므로 보지 않는다.
//  - PR: 대상 브랜치와의 공통 조상 이후 PR이 바꾼 내용 (origin/<대상>...HEAD)
//  - 그 밖: 마지막 성공 빌드 이후
// 비교 기준이 없으면(첫 빌드, 대상 브랜치 ref 없음) 바뀐 것으로 본다.
def codeChanged() {
    def range = null
    if (env.CHANGE_TARGET) {
        def target = "origin/${env.CHANGE_TARGET}"
        if (sh(script: "git rev-parse --verify --quiet ${target} > /dev/null", returnStatus: true) == 0) {
            range = "${target}...HEAD"
        }
    } else if (env.GIT_PREVIOUS_SUCCESSFUL_COMMIT) {
        range = "${env.GIT_PREVIOUS_SUCCESSFUL_COMMIT} HEAD"
    }
    if (!range) return true
    def rc = sh(script: "git diff --quiet ${range} -- ${env.APP_DIR}", returnStatus: true)
    // 0: 변경 없음, 1: 변경 있음, 그 밖(기준 커밋이 사라진 경우 등): 안전하게 실행
    return rc != 0
}

// 이미지의 ENTRYPOINT(aws, trivy 등)를 비우고 셸로 실행한다. HOME은 jenkins 사용자가 쓸 수 있는 곳으로 둔다.
def inContainer(String image, String args = '', Closure body) {
    docker.image(image).inside("--entrypoint= -e HOME=/tmp ${args}") { body() }
}

// 호스트 Docker 데몬을 쓰는 컨테이너 인자 (docker.sock 그룹 권한 포함)
def dockerSocketArgs() {
    def gid = sh(script: 'stat -c %g /var/run/docker.sock', returnStdout: true).trim()
    return "-v /var/run/docker.sock:/var/run/docker.sock --group-add ${gid}"
}

def withDocker(Closure body) {
    inContainer(env.DOCKER_IMAGE, dockerSocketArgs(), body)
}

def withAws(Closure body) {
    withCredentials([usernamePassword(credentialsId: env.AWS_CRED_ID,
                                      usernameVariable: 'AWS_ACCESS_KEY_ID', passwordVariable: 'AWS_SECRET_ACCESS_KEY')]) {
        inContainer(env.AWS_CLI_IMAGE, '', body)
    }
}

// IaC 레포를 얕게 받아 IAC_IMAGE_FILE의 processing_image_uri만 바꿔 main에 push한다.
// 파일 형식은 terraform_aws/scripts/publish-processing-image.sh가 쓰는 것과 같다.
// 다른 사람이 그사이 main에 push했다면 rebase 후 다시 시도한다.
def syncIacImage() {
    def iacDir = "${env.WORKSPACE}/.iac"
    withCredentials([usernamePassword(credentialsId: env.IAC_CRED_ID, usernameVariable: 'GIT_USER', passwordVariable: 'GIT_TOKEN')]) {
        // 토큰을 URL이나 .git/config에 남기지 않도록 askpass로 전달한다
        writeFile file: '.git-askpass.sh', text: '#!/bin/sh\ncase "$1" in Username*) echo "$GIT_USER" ;; *) echo "$GIT_TOKEN" ;; esac\n'
        withEnv(["GIT_ASKPASS=${env.WORKSPACE}/.git-askpass.sh", "GIT_TERMINAL_PROMPT=0", "IAC_DIR=${iacDir}"]) {
            sh 'chmod 700 .git-askpass.sh && rm -rf "$IAC_DIR" && git clone -q --depth 20 --branch "$IAC_BRANCH" "$IAC_REPO_URL" "$IAC_DIR"'
            writeFile file: "${iacDir}/${env.IAC_IMAGE_FILE}", text: "{\n  \"processing_image_uri\": \"${env.IMAGE_DIGEST_URI}\"\n}\n"
            sh '''
                cd "$IAC_DIR"
                # 저장소 .gitignore의 *.tfvars.json에 걸려도 이 파일만은 추적한다
                git add -f "$IAC_IMAGE_FILE"
                if git diff --cached --quiet; then
                    echo "IaC 레포가 이미 최신 digest를 가리킵니다."
                    exit 0
                fi
                git -c user.name="Jenkins" -c user.email="jenkins@dangubi.local" commit -q \
                    -m "chore(processing): sync processing_image_uri to ${IMAGE_DIGEST_URI##*@sha256:} [skip ci]" \
                    -m "Deployed by ${JOB_NAME} #${BUILD_NUMBER} from DangUBI_Lambda@${GIT_COMMIT}" \
                    -m "${BUILD_URL}"
                for attempt in 1 2 3; do
                    git push -q origin "HEAD:$IAC_BRANCH" && exit 0
                    echo "push 실패 (${attempt}/3) → 최신 main으로 rebase 후 재시도"
                    git pull -q --rebase origin "$IAC_BRANCH"
                done
                exit 1
            '''
        }
    }
}

// main 결과 알림. 어떤 커밋의 어느 빌드인지를 함께 붙인다.
def notifyMain(String color, String message) {
    slackSend(channel: env.SLACK_CHANNEL, color: color,
              message: "*${FUNCTION_NAME}* ${message}\n${commitLine()} · ${buildLink()}")
}

// Slack 링크. path가 없으면 빌드 페이지, 있으면 빌드 아래의 하위 경로(console, artifact/...)
def buildLink(String path = '', String label = null) {
    "<${env.BUILD_URL}${path}|${label ?: "#${env.BUILD_NUMBER}"}>"
}

def commitLine() {
    "`${env.GIT_COMMIT.take(7)}` ${slackEscape(env.COMMIT_SUBJECT)} (${slackEscape(env.COMMIT_AUTHOR)})"
}

def prLine() {
    "<${env.CHANGE_URL}|PR #${env.CHANGE_ID}> ${slackEscape(env.CHANGE_TITLE)} (${slackEscape(env.CHANGE_AUTHOR)})"
}

// repository@sha256:<64자> → sha256:<앞 12자>
def shortDigest(String uri) {
    def digest = uri?.contains('@') ? uri.substring(uri.indexOf('@') + 1) : (uri ?: '-')
    digest.length() > 19 ? digest.take(19) : digest
}

// Slack mrkdwn에서 특수 의미가 있는 문자를 바꾼다
def slackEscape(String text) {
    (text ?: '').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
}

def lambdaQuery(String query) {
    sh(script: "aws lambda get-function-configuration --function-name \"\$FUNCTION_NAME\" --query '${query}' --output text",
       returnStdout: true).trim()
}
