FROM python:3.10-slim
WORKDIR /app

COPY app/wiki_producer.py /app/wiki_producer.py
RUN pip install --no-cache-dir kafka-python pandas sseclient

CMD ["python", "/app/wiki_producer.py"]