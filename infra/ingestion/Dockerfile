# Outil d'ingestion : mêmes scripts que sur le poste, sans installer Python localement.
FROM python:3.11.10-slim
WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
ENTRYPOINT ["python", "-m"]
CMD ["ingestion.producer", "--help"]
