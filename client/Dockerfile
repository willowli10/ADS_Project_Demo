FROM python:3.9-slim-bullseye

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY client.py .
COPY 100_rep.txt .
COPY 1000_rep.txt .
COPY 10000_rep.txt .

CMD ["python", "client.py"]