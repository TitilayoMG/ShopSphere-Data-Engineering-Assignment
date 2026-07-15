FROM python:3.12-slim

WORKDIR /app

COPY pipeline/requirements.txt .

RUN cat requirements.txt

RUN pip install --default-timeout=600 --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "pipeline.main"]