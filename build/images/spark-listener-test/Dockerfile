ARG SPARK_BASE_IMAGE=apache/spark:4.1.2-scala2.13-java17-python3-ubuntu
FROM ${SPARK_BASE_IMAGE}

USER root
RUN mkdir -p /opt/spark/apps /opt/spark/metrics \
    && chmod -R 777 /opt/spark/apps /opt/spark/metrics
COPY context/jars/*.jar /opt/spark/jars/
COPY context/python-wheels /tmp/python-wheels
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-index --find-links=/tmp/python-wheels -r /tmp/requirements.txt \
    && rm -rf /tmp/python-wheels /tmp/requirements.txt
COPY context/conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY context/conf/log4j2.properties /opt/spark/conf/log4j2.properties
COPY context/listener-src /tmp/listener-src
RUN mkdir -p /tmp/listener-classes \
    && javac -cp "/opt/spark/jars/*" -d /tmp/listener-classes \
        /tmp/listener-src/com/dataship/spark/observability/SparkPlatformFirehoseListener.java \
    && jar cf /opt/spark/jars/dataship-spark-listener-test.jar -C /tmp/listener-classes . \
    && rm -rf /tmp/listener-src /tmp/listener-classes
USER spark
