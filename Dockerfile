FROM apache/superset:latest

USER root
RUN /usr/local/bin/pip install --target /app/.venv/lib/python3.10/site-packages --no-cache-dir psycopg2-binary pg8000
USER superset