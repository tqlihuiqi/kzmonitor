FROM python:2.7-alpine

RUN apk add --update nginx apache2-utils && rm -rf /var/cache/apk/*

COPY requirements.txt /kzmonitor/requirements.txt
RUN pip install -r /kzmonitor/requirements.txt

COPY . /kzmonitor
WORKDIR /kzmonitor

COPY docker/entrypoint.sh .
RUN chmod +x entrypoint.sh

COPY docker/nginx.conf /etc/nginx/nginx.conf
COPY docker/default.conf /etc/nginx/conf.d/default.conf

CMD ["/kzmonitor/entrypoint.sh"]
