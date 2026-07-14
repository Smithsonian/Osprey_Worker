FROM python:3.11-slim-bookworm

ARG JHOVE_VERSION=1.30.1

RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jre-headless \
        imagemagick \
        libimage-exiftool-perl \
        wget \
        zip \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/jhove \
    && wget -q "https://github.com/openpreserve/jhove/releases/download/v${JHOVE_VERSION}/jhove-${JHOVE_VERSION}.jar" \
        -O /opt/jhove/jhove.jar \
    && printf '#!/bin/sh\nexec java -jar /opt/jhove/jhove.jar "$@"\n' > /usr/local/bin/jhove \
    && chmod +x /usr/local/bin/jhove

WORKDIR /opt/osprey-worker

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY osprey_worker.py logutil.py ./
COPY worker/ worker/
COPY openzoom/ openzoom/

RUN mkdir -p logs

ENV OSPREY_SETTINGS=/config/settings.py

ENTRYPOINT ["python", "/opt/osprey-worker/osprey_worker.py"]
