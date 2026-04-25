FROM python:3.11-slim AS builder

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt


FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /root/.local /root/.local

COPY main.py analytics.py generate_data.py api_v1_pipeline.py ./

ENV PATH=/root/.local/bin:$PATH

ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/klucz_gcp.json

CMD ["python", "main.py"]
