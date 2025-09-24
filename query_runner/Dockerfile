FROM bitnami/spark:3.5

USER root
WORKDIR /app

# Minimal Python deps for kafka client
RUN install_packages python3 python3-pip && \
    pip3 install --no-cache-dir kafka-python==2.0.2

COPY service.py /app/service.py

# Will access spark-submit already in PATH from base image
CMD ["bash","-lc","python3 /app/service.py"] 