FROM python:3.8
COPY . .
RUN pip install --upgrade pip
RUN pip install elasticsearch
ENTRYPOINT ["python", "transfer.py"]