FROM python:3.12-slim AS base

WORKDIR /app/scraper

COPY ./../requirements.txt .

COPY ./../service-account.json .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "extract_data.py"]