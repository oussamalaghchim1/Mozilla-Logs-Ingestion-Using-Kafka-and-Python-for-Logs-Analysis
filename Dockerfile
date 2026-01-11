FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create directories
RUN mkdir -p /app/logs /app/output /app/src

# Setup default path
ENV PYTHONPATH="${PYTHONPATH}:/app/src"