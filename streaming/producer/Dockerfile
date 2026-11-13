FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY replay_clickstream.py .
ENTRYPOINT ["python", "replay_clickstream.py"]
