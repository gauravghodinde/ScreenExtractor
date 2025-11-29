# GPU-enabled worker image. Requires NVIDIA Container Toolkit on the host.
FROM nvidia/cuda:12.2.0-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive

# Basic deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv ffmpeg git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt ./
# If you need a specific torch + CUDA wheel, install here with the appropriate index URL.
RUN pip3 install --upgrade pip setuptools wheel && \
    pip3 install -r requirements.txt

# Copy source
COPY src ./src
WORKDIR /app/src

EXPOSE 50051

ENV PYTHONUNBUFFERED=1
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility,video

CMD ["python3", "-m", "scene_seeker_worker.worker"]
