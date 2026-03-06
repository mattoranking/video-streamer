FROM python:3.12-slim
WORKDIR /app
COPY streaming-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ ./shared/
COPY streaming-service/app.py .
EXPOSE 5004
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5004"]
