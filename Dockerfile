FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY "Реестр поставщиков информации от  2026-01-26.xlsx" .

CMD ["python", "-u", "main.py"]
