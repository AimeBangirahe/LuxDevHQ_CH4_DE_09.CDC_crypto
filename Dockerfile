FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Debezium dependencies
COPY requirements-debezium.txt .
RUN pip install --no-cache-dir -r requirements-debezium.txt

COPY . .

CMD ["python", "-u", "main.py"]
