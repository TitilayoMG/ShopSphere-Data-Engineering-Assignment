# for whole project run: docker compose up -d

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN cat requirements.txt

RUN pip install --default-timeout=600 --no-cache-dir -r requirements.txt

COPY . .
RUN chmod +x entrypoint.sh
ENTRYPOINT ["entrypoint.sh"]
# CMD ["python", "-m", "pipeline.main"]

# for only pipeline run: docker compose up pipeline --build
# FROM python:3.12-slim

# WORKDIR /app

# COPY requirements.txt .

# RUN cat requirements.txt

# RUN pip install --default-timeout=600 --no-cache-dir -r requirements.txt

# COPY . .

# CMD ["python", "-m", "pipeline.main"]