FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including ffmpeg for audio processing
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    git \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install shared package from git
RUN pip install --no-cache-dir git+https://github.com/CloudSound-MKNZ/cloudsound-shared.git@main

# Copy service requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY src /app/src

# Set Python path
ENV PYTHONPATH=/app

# Create temp directory for downloads
RUN mkdir -p /tmp/cloudsound-downloads

# Expose port
EXPOSE 8003

# Run the application
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8003"]
