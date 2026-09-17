# CLEXER Music - separate Railway service. Root directory: music/
FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg curl unzip ca-certificates && rm -rf /var/lib/apt/lists/*
# deno: yt-dlp's JavaScript runtime for YouTube's player challenge (without it some formats are missing)
RUN curl -fsSL https://deno.land/install.sh | DENO_INSTALL=/usr/local sh -s -- -y >/dev/null 2>&1 || true
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# yt-dlp must be current - YouTube changes break old versions. This layer is
# rebuilt on every deploy (the ARG changes with the git commit), never cached.
ARG RAILWAY_GIT_COMMIT_SHA=dev
RUN echo "build $RAILWAY_GIT_COMMIT_SHA" && pip install --no-cache-dir -U yt-dlp && python -c "import yt_dlp; print('yt-dlp', yt_dlp.version.__version__)"
COPY service.py .
CMD ["python", "service.py"]
