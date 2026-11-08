
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