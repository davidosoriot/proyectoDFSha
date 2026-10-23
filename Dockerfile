FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dfshacore ./dfshacore
COPY controlnode ./controlnode
COPY datanode ./datanode
COPY client ./client
