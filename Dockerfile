FROM python:3.11-slim

# Install system dependencies for PostgreSQL/Psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project structure
COPY . .

# Ensure logs directory exists
RUN mkdir -p /app/logs

# Set PYTHONPATH so 'from src.xxx' works
ENV PYTHONPATH=/app

# Command to run the local entrypoint
CMD ["python", "src/main.py"]