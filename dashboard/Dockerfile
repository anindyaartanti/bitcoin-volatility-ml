FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.36.0 \
    plotly==5.22.0 \
    pandas==2.2.2 \
    psycopg2-binary==2.9.9 \
    trino==0.329.0 \
    python-dotenv==1.0.1

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
