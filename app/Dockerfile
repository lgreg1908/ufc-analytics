# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy app and pipeline requirements
COPY app/requirements.txt ./requirements.txt
COPY pipeline/requirements.txt ./pipeline_requirements.txt

# Install dependencies from both requirements files
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r pipeline_requirements.txt

# Copy the application code and the pipeline folder into the container
COPY app/ ./
COPY pipeline ./pipeline


# Set PYTHONPATH so that modules under pipeline (i.e., pipeline/src) can be imported
ENV PYTHONPATH="/app/pipeline"

# Expose the port the app runs on
EXPOSE 8080

# Run the Dash app
CMD ["python", "app.py"]
