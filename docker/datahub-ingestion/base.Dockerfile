FROM python:3.9.9 as base

COPY ./base-requirements.txt requirements.txt

RUN pip install -r requirements.txt