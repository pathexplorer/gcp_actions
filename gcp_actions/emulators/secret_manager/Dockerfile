FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PORT=8083 \
    SM_EMULATOR_DATA_FILE=/data/secrets.json

WORKDIR /app

RUN pip install --no-cache-dir flask

COPY emulator.py .

# Create data directory with writable permissions
RUN mkdir -p /data && chmod 777 /data

EXPOSE 8083

CMD ["python", "emulator.py"]
