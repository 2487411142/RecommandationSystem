FROM spark:python3

USER root

WORKDIR /app
RUN pip install --no-cache-dir Flask

COPY ./backend .
COPY ./result /result

CMD ${SPARK_HOME}/bin/spark-submit web_backend.py