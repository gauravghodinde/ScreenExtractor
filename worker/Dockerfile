## CPU-only worker image (lighter and faster builds)
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Basic deps and ffmpeg
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg git gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt ./
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# Copy source
COPY src ./src
WORKDIR /app/src

EXPOSE 50051

ENV PYTHONUNBUFFERED=1

CMD ["python3", "-m", "scene_seeker_worker.worker"]
