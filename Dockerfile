FROM python:2.7-alpine

COPY requirements.txt /kzmonitor/requirements.txt
RUN pip install -r /kzmonitor/requirements.txt

COPY . /kzmonitor
WORKDIR /kzmonitor


CMD ["python", "kzmonitor.py"]
