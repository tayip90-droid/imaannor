FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Bağımlılıklar
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama
COPY . /app

# Koyeb/Cloud Run genelde PORT env verir; biz ona bağlanacağız
ENV PORT=8080
EXPOSE 8080

# Gunicorn ile production servis
CMD exec gunicorn -k gthread --threads 4 --timeout 120 -b 0.0.0.0:$PORT app:app
