FROM python:latest

WORKDIR app
COPY . /app

RUN cat requirements.txt
RUN python3 -m pip install -r requirements.txt