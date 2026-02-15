FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install python-telegram-bot==21.0 groq psycopg2-binary
CMD ["python", "main.py"]
