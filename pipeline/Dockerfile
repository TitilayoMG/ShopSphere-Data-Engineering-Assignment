# for whole project run: docker compose up -d

FROM python:3.12-slim

WORKDIR /app

COPY pipeline/requirements.txt .
# COPY requirements.txt .
RUN cat requirements.txt
# RUN python -m pip install --upgrade pip

RUN pip install --default-timeout=1800 --no-cache-dir psycopg2-binary==2.9.12
RUN pip install --default-timeout=1800 --no-cache-dir minio==7.2.20
RUN pip install --default-timeout=1800 --no-cache-dir python-dotenv==1.2.2
RUN pip install --default-timeout=1800 --no-cache-dir pyarrow==25.0.0
RUN pip install --default-timeout=1800 --no-cache-dir pymongo==4.17.0
RUN pip install --default-timeout=1800 --no-cache-dir pandas==3.0.3
RUN pip install --default-timeout=1800 --no-cache-dir requests==2.34.2
RUN pip install --default-timeout=1800 --no-cache-dir great_expectations==1.19.0

# RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chmod +x /app/pipeline/entrypoint.sh
ENTRYPOINT ["/app/pipeline/entrypoint.sh"]



# CMD ["python", "-m", "pipeline.main"]

# for only pipeline run: docker compose up pipeline --build
# FROM python:3.12-slim

# WORKDIR /app

# COPY requirements.txt .

# RUN cat requirements.txt

# RUN pip install --default-timeout=600 --no-cache-dir -r requirements.txt

# COPY . .

# CMD ["python", "-m", "pipeline.main"]