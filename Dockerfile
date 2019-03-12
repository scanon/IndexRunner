FROM python:3.7-slim

MAINTAINER KBase Developer

# Install pip requirements
ARG DEVELOPMENT
COPY requirements.txt dev-requirements.txt /tmp/

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    if [ "$DEVELOPMENT" ]; then pip install --no-cache-dir -r /tmp/dev-requirements.txt; fi && \
    rm /tmp/*requirements.txt

RUN apt-get update && \
    apt-get install -y wget

# Install dockerize
ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/kbase/dockerize/raw/master/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    tar -C /usr/local/bin -xvzf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

COPY . /app

WORKDIR /app

ENV PYTHONPATH=/app/lib
ENV KB_DEPLOYMENT_CONFIG=/app/deploy.cfg

CMD [ "./entrypoint.sh" ]
