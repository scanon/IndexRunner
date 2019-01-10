#
# Kafka Event Handler
# This waits for events and dispatches it to the indexer
#
from confluent_kafka import Producer
import json
import logging


class EventProducer():

    def __init__(self, config):
        self.topic = config.get('kafka-index-topic', 'wsevents')
        server = config.get('kafka-server', 'kafka')
        config = config
        self.log = logging.getLogger('indexrunner')
        if server is not None:
            self.prod = Producer({'bootstrap.servers': server})

    def index_objects(self, objects, public=False):
        for obj in objects:
            (objtype, objtypever) = obj[2].split('-')
            evt = {
                'strcde': 'WS',
                'accgrp': obj[6],
                'objid': '%s' % (obj[0]),
                'ver': obj[4],
                'newname': None,
                'evtype': 'NEW_VERSION',
                'time': obj[3],
                'objtype': objtype,
                'objtypever': objtypever,
                'public': public
                }
            data = json.dumps(evt)
            self.prod.produce(self.topic, data.encode('utf-8'))
        self.prod.flush()
