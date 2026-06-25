# Standard glibc base — apt's libgomp1 lands on the default ld.so search
# path, so LightGBM's lib_lightgbm.so finds libgomp.so.1 at import time.
# This sidesteps the Nixpacks dynamic-loader problem (see nixpacks.toml note).
FROM python:3.13-slim

# OpenMP runtime required by LightGBM; libstdc++ for scipy/numpy/shap wheels.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default process. The Railway WEB service overrides this with start command
# `streamlit run app.py` (port/address pinned in .streamlit/config.toml — no
# $PORT, since the start command runs without a shell to expand it). The
# WORKER uses this default.
CMD ["python", "cron.py"]
