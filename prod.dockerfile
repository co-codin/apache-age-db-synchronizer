FROM python:3.8.7
ARG SERVICE_PORT=8081

COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -U pip && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt

COPY migration_service /app/migration_service
WORKDIR /app

EXPOSE $SERVICE_PORT
CMD ["python3", "-m", "migration_service"]