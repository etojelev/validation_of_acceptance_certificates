FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && pip install uv

WORKDIR /app

COPY pyproject.toml ./

RUN uv pip install -r pyproject.toml --system --no-cache

COPY . /app

EXPOSE 8009

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8009"]