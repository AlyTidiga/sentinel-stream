FROM python:3.11-slim

WORKDIR /app

RUN pip install fastapi uvicorn xgboost scikit-learn numpy prometheus-fastapi-instrumentator

COPY models/ models/
COPY serve.py .

EXPOSE 8000

CMD ["uvicorn", "serve:app", "--host", "0.0.0.0", "--port", "8000"]
