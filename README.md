# IndexRunner

This is an index runner.  It watches a Kafka topic and can trigger indexing using indexers written with the SDK.  The results go into ElasticSearch.

# Setting up Test Environment

Make sure python3 is the default python and the required packages are installed.

Create a test.cfg and set `KB_DEPLOYMENT_CONFIG` to point to it.

```
export KB_DEPLOYMENT_CONFIG=test.cfg
```


## Setup and test

1: Start Kafka and Elastic

```
docker run -d --name es5 -v es5:/usr/share/elasticsearch/data/ -e "xpack.security.enabled=false" -e "ES_JAVA_OPTS=-Xms512m -Xmx512m"  -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:5.6.9

docker run -d --name=zookeeper -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper:5.0.0

docker run -d --name=kafka --link zookeeper -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e "AFKA_HEAP_OPTS=-Xmx512m -Xms512m" confluentinc/cp-kafka:5.0.0

```

2: Building Mock Image

```
cd test/mock_indexer
docker build -t mock_indexer .
```

3: Setup virtualenv

```
virtualenv IndexRunner_venv
source IndexRunner_venv/bin/activate
pip install -r requirements.txt
```

4: Run tests

```
export KB_DEPLOYMENT_CONFIG=test.cfg
export KB_AUTH_TOKEN=bogus
make test
```

# Notes

I still don't really understand `lastin` and `extpub` but it looks like neither of
these are used by the search api.
