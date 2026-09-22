FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY your_script.py .
COPY data ./data

RUN mkdir -p /app/output

CMD ["python", "your_script.py", "--input-dir", "/app/data", "--output-dir", "/app/output"]
