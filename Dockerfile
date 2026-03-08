# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Install system-level dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container
COPY . .

# Install necessary Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Expose Gradio's default port
EXPOSE 7860

# Ensure Gradio listens on all network interfaces
ENV GRADIO_SERVER_NAME="0.0.0.0"

# Run the app
CMD ["python", "app_demo.py"]
