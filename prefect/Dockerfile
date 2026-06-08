FROM prefecthq/prefect:2-python3.12

USER root

# System dependencies: Node.js + Chromium untuk tweet-harvest
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl gnupg \
      libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
      libcups2 libdrm2 libdbus-1-3 libxkbcommon0 \
      libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
      libgbm1 libpango-1.0-0 libcairo2 libasound2 \
    && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    npm install -g tweet-harvest@latest && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --ignore-requires-python -r /tmp/requirements.txt

WORKDIR /opt/prefect
