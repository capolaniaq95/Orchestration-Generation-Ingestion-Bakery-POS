FROM apache/airflow:3.1.5-python3.11

# Install additional system dependencies if needed (including build tools)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (as root to ensure it lands correctly)
COPY requirements.txt /opt/airflow/requirements.txt

# Install Python packages as airflow user
USER airflow
RUN pip install --no-cache-dir --user -r /opt/airflow/requirements.txt

# Ensure user-local bin is on PATH
ENV PATH="/home/airflow/.local/bin:${PATH}"
