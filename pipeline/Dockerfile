# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy only requirements first (better caching)
COPY pipeline/requirements.txt /app/requirements.txt

# Install dependencies before copying code (improves build speed)
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# Copy the entire pipeline folder (except credentials)
COPY pipeline /app/pipeline

# Copy the config folder (contains config.yaml)
COPY pipeline/config /app/config

# Set Python path
ENV PYTHONPATH="/app/pipeline"

# Allow running arbitrary scripts from the scripts/ folder
ENTRYPOINT ["python", "-m"]
