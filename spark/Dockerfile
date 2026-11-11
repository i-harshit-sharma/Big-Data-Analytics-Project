FROM apache/spark:3.5.3-python3
USER root
RUN pip3 install --no-cache-dir happybase vaderSentiment langdetect numpy
# Kafka connector jars matching Spark 3.5.3 (Scala 2.12)
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar
COPY app /opt/app
RUN mkdir -p /checkpoints && chown -R spark:spark /checkpoints /opt/app
ENV PYTHONPATH=/opt/app PYSPARK_PYTHON=python3
USER spark
