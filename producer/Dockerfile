FROM python:3.11-slim
RUN pip install --no-cache-dir kafka-python-ng requests
COPY producer.py /app/producer.py
CMD ["python", "-u", "/app/producer.py"]
