FROM python:3.12-slim
 
WORKDIR /app
 
# instala dependências primeiro (cache de layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
# copia o código ETL
COPY . .
 
ENTRYPOINT ["python", "main.py"]
CMD ["run"]
 