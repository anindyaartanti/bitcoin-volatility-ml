FROM apache/spark:3.5.1
USER root
RUN pip install --no-cache-dir \
    psycopg2-binary pybreaker requests \
    "mlflow==2.10.2" "xgboost==2.0.3" "scikit-learn==1.3.2" \
    joblib "confluent-kafka==2.2.0" numpy cryptography
