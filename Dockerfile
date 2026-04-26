FROM python:3.12-slim

WORKDIR /app

# System dependencies — lxml needs libxml2/libxslt; gcc for native wheels.
RUN apt-get update && apt-get install -y \
    libxml2-dev \
    libxslt-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first so they cache between code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source.
COPY . .

# Render injects the listening port via the PORT env var. We document 8000
# here for local docker runs; the FastAPI app reads PORT at startup.
EXPOSE 8000

# Default command: start the FastAPI app (webhook listener + scheduler).
# The scheduler's cron jobs only register if AUTO_SEARCH_ENABLED=true.
# Override with `python main.py <command>` for one-off CLI runs.
CMD ["sh", "-c", "uvicorn webhook:app --host 0.0.0.0 --port ${PORT:-8000}"]
