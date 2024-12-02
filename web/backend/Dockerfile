FROM spark:python3

USER root

WORKDIR /app
RUN pip install --no-cache-dir Flask pandas

COPY ./backend .
COPY ./result /result

CMD ${SPARK_HOME}/bin/spark-submit web_backend.py