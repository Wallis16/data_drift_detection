# Use an official Python runtime as a parent image
FROM python:3.10-slim

WORKDIR /mlflow

# Copy the current directory contents into the container at /app
COPY . /mlflow

# Install any needed dependencies specified in requirements.txt
RUN pip install --no-cache-dir mlflow==2.10.0

# Expose the MLflow UI port
EXPOSE 5000

# Run MLflow server when the container launches
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000", "--backend-store-uri", "sqlite:///mlflow.db"]
