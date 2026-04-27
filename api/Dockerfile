FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --retries 10 --timeout 120 -r requirements.txt
COPY . .
CMD ["sh", "-c", "gunicorn main:app -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:${API_PORT:-8000} --workers ${API_WORKERS:-4} --threads ${API_THREADS:-1} --timeout ${API_TIMEOUT_SECONDS:-120}"]
