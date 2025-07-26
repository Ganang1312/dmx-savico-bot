FROM python:3.10-slim

RUN apt-get update && apt-get install -y wget unzip curl chromium-driver chromium

ENV PATH="/usr/lib/chromium/:${PATH}"
ENV CHROME_BIN=/usr/bin/chromium

COPY . /app
WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
