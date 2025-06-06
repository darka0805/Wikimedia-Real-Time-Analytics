
FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir fastapi cassandra-driver uvicorn python-dateutil redis
CMD ["uvicorn", "mainB:app", "--host", "0.0.0.0", "--port", "8000"]
