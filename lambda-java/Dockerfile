FROM gradle:9.8.2-jdk21 AS build
WORKDIR /workspace
COPY settings.gradle build.gradle ./
COPY src ./src
RUN gradle --no-daemon test stage

FROM public.ecr.aws/lambda/java:21
COPY --from=build /workspace/build/lambda/lib/ ${LAMBDA_TASK_ROOT}/lib/
CMD ["com.ubi.processing.PreprocessHandler::handleRequest"]
