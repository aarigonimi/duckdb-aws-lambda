# Official AWS Lambda Python base image.
# Runtime and deprecation dates: https://docs.aws.amazon.com/lambda/latest/dg/python-image.html
# Python 3.13 is used instead of 3.14/3.15 so the pinned DuckDB 1.5.5
# manylinux wheel is a conservative, well-established combination.
FROM public.ecr.aws/lambda/python:3.13

# DuckDB native extensions are installed at image build time into a path that
# remains readable in Lambda. Lambda's function filesystem is read-only except
# /tmp; setting extension_directory avoids installing into $HOME at runtime.
# https://duckdb.org/docs/stable/extensions/installing_extensions.html
# Direct extension download for Lambda/containers is documented at:
# https://duckdb.org/docs/stable/extensions/advanced_installation_methods.html
ENV DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb_extensions
ENV HOME=/tmp

COPY src/requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt
RUN pip3 install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt --target "${LAMBDA_TASK_ROOT}"

RUN mkdir -p /opt/duckdb_extensions /tmp \
    && python3 -c 'import duckdb; con = duckdb.connect(config={"extension_directory": "/opt/duckdb_extensions", "home_directory": "/tmp"}); con.install_extension("httpfs"); con.install_extension("aws"); con.install_extension("json"); con.close()'

COPY src/lambda_function.py ${LAMBDA_TASK_ROOT}/lambda_function.py

# Handler format required by the AWS Python base image:
# https://docs.aws.amazon.com/lambda/latest/dg/python-image.html
CMD ["lambda_function.handler"]
