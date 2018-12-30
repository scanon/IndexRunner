FROM python:3

MAINTAINER KBase Developer

ADD requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

RUN wget https://github.com/kbase/dockerize/raw/master/dockerize-linux-amd64-v0.6.1.tar.gz && \
    tar xvzf dockerize-linux-amd64-v0.6.1.tar.gz && \
    mv dockerize /usr/bin && \
    rm *.tar.gz

ADD . /app

WORKDIR /app

ENV PYTHONPATH=/app/lib
ENV KB_DEPLOYMENT_CONFIG=/app/deploy.cfg

CMD [ "./entrypoint.sh" ]
