FROM python:3.12-slim
WORKDIR /app
COPY user-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ ./shared/
COPY user-service/src/ ./src/
EXPOSE 5001
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "5001"]
