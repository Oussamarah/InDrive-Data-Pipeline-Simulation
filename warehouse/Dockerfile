FROM python:3.11-slim
WORKDIR /opt/warehouse
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY init_warehouse.py .
CMD ["python", "init_warehouse.py"]
