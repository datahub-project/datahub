FROM python:3.8 as base

COPY ./base-requirements.txt requirements.txt

RUN pip install -r requirements.txt