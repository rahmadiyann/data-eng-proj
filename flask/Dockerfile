FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "app:app"]