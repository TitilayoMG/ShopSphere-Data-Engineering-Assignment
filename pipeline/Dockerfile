FROM python:3.12-slim

WORKDIR /app

COPY pipeline/requirements.txt .

# RUN python -m pip install --upgrade pip

RUN pip install --default-timeout=1200 --no-cache-dir -r requirements.txt

COPY . .
RUN chmod +x /app/pipeline/entrypoint.sh
ENTRYPOINT ["/app/pipeline/entrypoint.sh"]