# Build from the repository root:
# docker build --progress=plain -f application/flink/Dockerfile -t flink-reference-jobs:2.2.0-dev .

ARG FLINK_BASE_IMAGE=flink:2.2.0
FROM ${FLINK_BASE_IMAGE}

USER root

# Install system dependencies.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-venv \
        python3-pip \
        build-essential \
        cmake \
        ca-certificates \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Create a Python virtual environment.
RUN python3 -m venv /opt/flink/venv

# Upgrade pip.
RUN /opt/flink/venv/bin/python -m pip install --no-cache-dir --upgrade pip

# Install PyFlink and YAML dependencies.
RUN /opt/flink/venv/bin/python -m pip install --no-cache-dir \
    apache-flink==2.2.0 \
    PyYAML==6.0.2

# Install Couchbase separately to isolate build errors.
RUN /opt/flink/venv/bin/python -m pip install --no-cache-dir \
    couchbase==4.3.2

# Download the Kafka connector.
RUN curl -fsSL \
    -o /opt/flink/lib/flink-connector-kafka-5.0.0-2.2.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.2/flink-connector-kafka-5.0.0-2.2.jar

# Configure Python for PyFlink.
ENV PATH="/opt/flink/venv/bin:${PATH}"
ENV PYFLINK_CLIENT_EXECUTABLE=/opt/flink/venv/bin/python
ENV PYFLINK_PYTHON=/opt/flink/venv/bin/python

# Application configuration.
ENV PYTHONPATH=/opt/flink/usrlib/application/src
ENV PIPELINE_CONFIG=/opt/flink/usrlib/application/config/jobs.yaml

# Copy application files.
COPY application/flink/src/ /opt/flink/usrlib/application/src/
COPY config/jobs.yaml /opt/flink/usrlib/application/config/jobs.yaml

USER flink
