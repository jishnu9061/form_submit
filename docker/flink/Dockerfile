# =============================================================================
# Flink image with MySQL JDBC driver + Kafka connector JARs pre-installed.
# We build on top of the official Flink 1.18 image and add connector JARs.
# =============================================================================
FROM flink:1.18-java11

# Version pins — change here if upgrading
ENV FLINK_VERSION=1.18.1
ENV SCALA_VERSION=2.12
ENV KAFKA_CONNECTOR_VERSION=3.1.0-1.18
ENV JDBC_CONNECTOR_VERSION=3.1.2-1.18
ENV MYSQL_DRIVER_VERSION=8.0.33

# Download connector JARs into Flink's lib directory so they are available
# to all jobs without needing to submit them manually.
RUN wget -q -P /opt/flink/lib \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
    && wget -q -P /opt/flink/lib \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar" \
    && wget -q -P /opt/flink/lib \
    "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_DRIVER_VERSION}/mysql-connector-j-${MYSQL_DRIVER_VERSION}.jar"
