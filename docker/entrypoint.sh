#!/bin/sh

user=${USER:-"admin"}
pass=${PASS:-"567"}

htpasswd -c -b /etc/nginx/.htpasswd $user $pass

if [ -n "$PORT" ]; then
  sed -e "s/8080/$PORT/" -i /etc/nginx/conf.d/default.conf
fi

nginx

python kzmonitor.py
