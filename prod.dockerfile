FROM python:3.8.7

COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

COPY migration_service /app/migration_service
WORKDIR /app

CMD ["python3", "-m", "migration_service"]