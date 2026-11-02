FROM public.ecr.aws/lambda/python:3.13 AS test
COPY requirements.txt requirements-dev.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY src ./src
COPY tests ./tests
RUN python -m pytest -q

FROM public.ecr.aws/lambda/python:3.13
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -t ${LAMBDA_TASK_ROOT}
# Copying from the test stage keeps tests in the build path.
COPY --from=test ${LAMBDA_TASK_ROOT}/src/preprocess.py ${LAMBDA_TASK_ROOT}/
CMD ["preprocess.handler"]
