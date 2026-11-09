# ============================================================
# Stage 1: Build Java components and resolve dependencies
# ============================================================
FROM maven:3.9-eclipse-temurin-17 AS java-builder

WORKDIR /build

# ------------------------------------------------------------
# Build the Couchbase Java sink
# ------------------------------------------------------------
COPY pom.xml .
COPY java/src/main/java/ java/src/main/java/

RUN mvn -B clean package

# ------------------------------------------------------------
# Resolve Kafka connector runtime dependencies
# ------------------------------------------------------------
COPY kafka-pom.xml .

RUN mvn -B -f kafka-pom.xml \
    dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=/build/kafka-deps

# ------------------------------------------------------------
# Resolve Hadoop Common runtime dependencies
# Required for Hadoop FileSystem support (file://)
# and org.apache.hadoop.conf.Configuration.
#
# Explicitly pin commons-cli to 1.5.0.
# Flink 2.2.0 requires this version.
# Hadoop Common 2.10.2 can otherwise introduce
# commons-cli 1.2 transitively.
# ------------------------------------------------------------
RUN printf '%s\n' \
    '<project xmlns="http://maven.apache.org/POM/4.0.0"' \
    '         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' \
    '         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">' \
    '    <modelVersion>4.0.0</modelVersion>' \
    '    <groupId>com.xlen.flink</groupId>' \
    '    <artifactId>hadoop-runtime-dependencies</artifactId>' \
    '    <version>1.0.0</version>' \
    '    <dependencies>' \
    '        <dependency>' \
    '            <groupId>org.apache.hadoop</groupId>' \
    '            <artifactId>hadoop-common</artifactId>' \
    '            <version>2.10.2</version>' \
    '        </dependency>' \
    '        <dependency>' \
    '            <groupId>commons-cli</groupId>' \
    '            <artifactId>commons-cli</artifactId>' \
    '            <version>1.5.0</version>' \
    '        </dependency>' \
    '    </dependencies>' \
    '</project>' \
    > /build/hadoop-pom.xml

# Verify resolved Commons CLI dependency
RUN mvn -B -f /build/hadoop-pom.xml \
    dependency:tree \
    -Dincludes=commons-cli:commons-cli

# Copy Hadoop runtime dependencies
RUN mvn -B -f /build/hadoop-pom.xml \
    dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=/build/hadoop-deps

# Ensure Maven copied the expected version
RUN test -f /build/hadoop-deps/commons-cli-1.5.0.jar && \
    test "$(find /build/hadoop-deps -maxdepth 1 -name 'commons-cli-*.jar' | wc -l)" -eq 1

# ------------------------------------------------------------
# Build Azure Workload Identity Token Provider
# ------------------------------------------------------------
COPY azure-provider/pom.xml ./azure-provider/pom.xml
COPY azure-provider/src/ ./azure-provider/src/

RUN mvn -B -f azure-provider/pom.xml clean package


# ============================================================
# Stage 2: Flink Application Mode runtime
# ============================================================
FROM flink:2.2.0

USER root

# ------------------------------------------------------------
# Install Python and native dependencies
# ------------------------------------------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-venv \
        python3-pip \
        build-essential \
        cmake \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------
# Create Python virtual environment
# ------------------------------------------------------------
RUN python3 -m venv /opt/flink/venv

RUN /opt/flink/venv/bin/python -m pip install --no-cache-dir --upgrade pip

# ------------------------------------------------------------
# Install PyFlink and YAML support
# ------------------------------------------------------------
RUN /opt/flink/venv/bin/python -m pip install --no-cache-dir \
    apache-flink==2.2.0 \
    PyYAML==6.0.2

# ------------------------------------------------------------
# Install Python Couchbase SDK
# ------------------------------------------------------------
RUN /opt/flink/venv/bin/python -m pip install --no-cache-dir \
    couchbase==4.3.2

# ------------------------------------------------------------
# Install Java Couchbase sink
# ------------------------------------------------------------
COPY --from=java-builder \
    /build/target/flink-reference-jobs-2.2.0-all.jar \
    /opt/flink/usrlib/couchbase-sink-all.jar

# ------------------------------------------------------------
# Install Kafka connector dependencies
# ------------------------------------------------------------
COPY --from=java-builder \
    /build/kafka-deps/ \
    /opt/flink/lib/

# ------------------------------------------------------------
# Install Hadoop Common and runtime dependencies
# Provides Hadoop FileSystem classes, including file://
# ------------------------------------------------------------
COPY --from=java-builder \
    /build/hadoop-deps/ \
    /opt/flink/lib/

# ------------------------------------------------------------
# Enforce Commons CLI version required by Flink 2.2.0
# Remove any other version that may have been introduced
# by Kafka or Hadoop runtime dependencies.
# ------------------------------------------------------------
RUN set -eux; \
    test -f /opt/flink/lib/commons-cli-1.5.0.jar; \
    find /opt/flink/lib -maxdepth 1 -type f \
        -name 'commons-cli-*.jar' \
        ! -name 'commons-cli-1.5.0.jar' \
        -delete; \
    test "$(find /opt/flink/lib -maxdepth 1 -name 'commons-cli-*.jar' | wc -l)" -eq 1; \
    ls -l /opt/flink/lib/commons-cli-*.jar

# ------------------------------------------------------------
# Configure Azure Filesystem plugin
# ------------------------------------------------------------

# Create dedicated Flink plugin directory.
RUN mkdir -p /opt/flink/plugins/azure-fs-hadoop

# Copy the official Flink Azure filesystem plugin.
RUN cp /opt/flink/opt/flink-azure-fs-hadoop-2.2.0.jar \
    /opt/flink/plugins/azure-fs-hadoop/

# Copy custom Azure Workload Identity provider.
COPY --from=java-builder \
    /build/azure-provider/target/flink-azure-workload-identity-1.0.0.jar \
    /opt/flink/plugins/azure-fs-hadoop/

# Copy Azure Identity runtime dependencies.
COPY --from=java-builder \
    /build/azure-provider/target/dependency/ \
    /opt/flink/plugins/azure-fs-hadoop/

# Remove native libraries for unsupported platforms.
RUN rm -f \
    /opt/flink/plugins/azure-fs-hadoop/*osx* \
    /opt/flink/plugins/azure-fs-hadoop/*macos* \
    /opt/flink/plugins/azure-fs-hadoop/*windows* \
    /opt/flink/plugins/azure-fs-hadoop/*aarch_64* \
    /opt/flink/plugins/azure-fs-hadoop/*kqueue*

# ------------------------------------------------------------
# Configure PyFlink
# ------------------------------------------------------------
ENV PATH="/opt/flink/venv/bin:${PATH}"
ENV PYFLINK_CLIENT_EXECUTABLE=/opt/flink/venv/bin/python
ENV PYFLINK_PYTHON=/opt/flink/venv/bin/python
ENV PYTHONPATH=/opt/flink/usrlib/application/src
ENV PIPELINE_CONFIG=/opt/flink/usrlib/application/config/jobs.yaml

# ------------------------------------------------------------
# Copy application source and configuration
# ------------------------------------------------------------
COPY src/ /opt/flink/usrlib/application/src/
COPY config/jobs.yaml /opt/flink/usrlib/application/config/jobs.yaml

USER flink