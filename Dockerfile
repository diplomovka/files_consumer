FROM python:3.8.16-slim-bullseye

WORKDIR /app

COPY requirements.txt /app

RUN pip install -r requirements.txt

COPY . ./

CMD [ "python", "./files_consumer.py" ]