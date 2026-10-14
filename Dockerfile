FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Prevent Python from writing bytecode and buffer output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install Python dependencies first for better build caching
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the application source code
COPY app/ ./app/

# Default command: start the FastAPI application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
