# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy only requirements first (better caching)
COPY requirements.txt /app/requirements.txt

# Install dependencies before copying code (improves build speed)
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# Copy the entire code (all files and folders in the build context)
COPY . /app/pipeline

# Copy the config folder to a separate location if required
COPY config /app/config

# Set Python path so that modules in /app/pipeline are discoverable
ENV PYTHONPATH="/app/pipeline"

# Allow running arbitrary scripts from the scripts/ folder
ENTRYPOINT ["python", "-m"]
CMD ["pipeline.scripts.dummy"]
