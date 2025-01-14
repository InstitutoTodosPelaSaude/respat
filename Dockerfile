FROM python:3.11-slim-bookworm

ENV TZ=America/Sao_Paulo

WORKDIR /usr/app/respiratorios/

COPY requirements.txt /usr/app/respiratorios/
COPY dbt/ /usr/app/respiratorios/dbt/

RUN apt update
RUN pip install -r requirements.txt
WORKDIR /usr/app/respiratorios/