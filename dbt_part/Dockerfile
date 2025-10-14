FROM python:3.12-slim AS base

WORKDIR /app/dbt_part

COPY ./../requirements.txt .

COPY ./../service-account.json .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DBT_PROFILES_DIR=/app/dbt_part/.dbt

CMD ["dbt", "run"]