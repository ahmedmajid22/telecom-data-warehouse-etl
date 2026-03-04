FROM python:3.11-slim

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code and data
COPY src/ ./src/
COPY data/ ./data/

# Create logs directory
RUN mkdir -p /app/logs 
# Set working directory to src so imports work as expected
WORKDIR /app/src

CMD ["python", "main.py"]