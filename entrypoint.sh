#!/bin/sh

if [ ! -z "$CONF_URL" ] ; then
   ENV="-env $CONF_URL"
fi

if [ ! -z "$SECRET" ] ; then
  ENVHEADER="--env-header /run/secrets/$SECRET"
fi

/usr/bin/dockerize  \
  $ENV \
  $ENVHEADER \
  -validate-cert=false \
  -template /app/deploy.cfg.templ:/app/deploy.cfg

if [ $# -gt 0 ] ; then
  exec $@
else
  exec  python app.py
fi
