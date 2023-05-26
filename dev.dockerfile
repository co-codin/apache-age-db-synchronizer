FROM python:3.10.11

COPY requirements.txt requirements.dev.txt /tmp/
RUN pip3 install --no-cache-dir -U pip &&  \
    pip3 install --no-cache-dir -r /tmp/requirements.dev.txt

WORKDIR /app
CMD ["python3", "-m", "migration_service"]