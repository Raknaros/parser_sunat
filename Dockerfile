FROM python:3.12-slim

# Evitar que Python escriba archivos .pyc y forzar logs sin buffer
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Instalar dependencias del sistema para lxml y psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY src/ /app/src/

# Crear usuario sin privilegios por seguridad
RUN adduser --disabled-password --gecos "" appuser
USER appuser

# Exponer el puerto de la API
EXPOSE 10022

# Comando de inicio
# Workers=1 porque el paralelismo ya se maneja internamente con ThreadPoolExecutor en engine.py
# Multiplicar workers de Uvicorn multiplicaría el ThreadPool, saturando RAM y BD
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "10022", "--workers", "1"]