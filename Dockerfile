FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md requirements.txt ./
COPY src ./src
COPY data ./data
COPY reports ./reports
COPY airbyte ./airbyte
COPY docs ./docs
COPY sql ./sql
COPY .env.example ./.env.example

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

ENTRYPOINT ["retail-sales-pipeline"]
CMD ["--help"]