# Hive Metastore with S3 (MinIO) support and a Postgres backend.
# Jar versions match the Hadoop 3.1.0 libraries shipped with Hive 3.1.3.
ARG HIVE_VERSION=3.1.3

FROM curlimages/curl:8.10.1 AS jars
ARG HADOOP_AWS_VERSION=3.1.0
ARG AWS_SDK_VERSION=1.11.271
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN mkdir -p /tmp/jars && cd /tmp/jars && \
    curl -fSLO "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -fSLO "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" && \
    curl -fSLO "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

FROM apache/hive:${HIVE_VERSION}
COPY --from=jars /tmp/jars/*.jar /opt/hive/lib/
