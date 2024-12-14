# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies for sqlite
RUN apt-get update && apt-get install -y \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire preprocessor directory
COPY preprocessor/ .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Create intermediate directory
RUN mkdir -p intermediate

# Define environment variable
ENV PYTHONUNBUFFERED=1

# Run preprocessor.py when the container launches
CMD ["python", "preprocessor.py"]