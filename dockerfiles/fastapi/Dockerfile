FROM python:3.8-slim
WORKDIR /app
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY . .
COPY . /app
EXPOSE 8800
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8800", "--reload"]
