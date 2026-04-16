ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:latest
FROM ${BUILD_FROM}

ARG BUILD_ARCH=amd64
ARG BUILD_VERSION=1.0.0

# Install Python + aiohttp via apk (avoids PEP 668 pip conflict on Alpine 3.18+)
RUN apk add --no-cache \
    python3 \
    py3-pip \
    py3-aiohttp \
    py3-multidict \
    py3-yarl \
    py3-frozenlist \
    py3-aiosignal \
    curl \
    ca-certificates \
    tzdata

# aiofiles only available via pip
RUN pip3 install --no-cache-dir --break-system-packages aiofiles==23.2.1

# Copy addon files
COPY rootfs /

RUN chmod a+x /opt/apex/server.py \
    && chmod a+x /opt/apex/run.sh

LABEL \
    io.hass.name="APEX Trading AI" \
    io.hass.description="AI trading bot with Claude AI, live market data, and broker integration" \
    io.hass.arch="${BUILD_ARCH}" \
    io.hass.type="addon" \
    io.hass.version="${BUILD_VERSION}"

# Tell HAOS what to run when the addon starts.
# With init: false in config.yaml, HAOS runs this CMD directly as PID 1.
CMD ["/opt/apex/run.sh"]
