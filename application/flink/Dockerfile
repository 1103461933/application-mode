# Stage 1: Build the Java Couchbase sink and resolve Kafka dependencies.
FROM maven:3.9-eclipse-temurin-17 AS java-builder

WORKDIR /build

# Build the Couchbase sink.
COPY pom.xml .
COPY java/src/main/java/ java/src/main/java/

RUN mvn -B clean package

# Resolve Kafka connector runtime dependencies.
COPY kafka-pom.xml .

RUN mvn -B -f kafka-pom.xml \
    dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=/build/kafka-deps


# Stage 2: Flink Application Mode runtime.
FROM flink:2.2.0

USER root

# Install Python and native dependencies.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-venv \
        python3-pip \
        build-essential \
        cmake \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create Python virtual environment.
RUN python3 -m venv /opt/flink/venv

RUN /opt/flink/venv/bin/python -m pip install --no-cache-dir --upgrade pip

# Install PyFlink and YAML support.
RUN /opt/flink/venv/bin/python -m pip install --no-cache-dir \
    apache-flink==2.2.0 \
    PyYAML==6.0.2

# Install Python Couchbase SDK.
RUN /opt/flink/venv/bin/python -m pip install --no-cache-dir \
    couchbase==4.3.2

# Install the Java Couchbase sink.
COPY --from=java-builder \
    /build/target/flink-reference-jobs-2.2.0-all.jar \
    /opt/flink/usrlib/couchbase-sink-all.jar

# Install the Kafka connector and its resolved runtime dependencies.
COPY --from=java-builder \
    /build/kafka-deps/ \
    /opt/flink/lib/

# Configure PyFlink.
ENV PATH="/opt/flink/venv/bin:${PATH}"
ENV PYFLINK_CLIENT_EXECUTABLE=/opt/flink/venv/bin/python
ENV PYFLINK_PYTHON=/opt/flink/venv/bin/python
ENV PYTHONPATH=/opt/flink/usrlib/application/src
ENV PIPELINE_CONFIG=/opt/flink/usrlib/application/config/jobs.yaml

# Copy application source and configuration.
COPY src/ /opt/flink/usrlib/application/src/
COPY config/jobs.yaml /opt/flink/usrlib/application/config/jobs.yaml

USER flink

