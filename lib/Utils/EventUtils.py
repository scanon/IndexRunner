#
# Kafka Event Handler
# This waits for events and dispatches it to the indexer
#
from confluent_kafka import Consumer, KafkaError
import json
from threading import Thread
from Utils.IndexerUtils import IndexerUtils


class EventHandler:

    def __init__(self, config):
        self.topic = config.get('kafka-topic', 'wsevents')
        self.server = config.get('kafka-server', 'kafka')
        self.cgroup = config.get('kafka-clientgroup', 'search_indexer')
        self.kafka_thread = Thread(target=self._kafka_watcher)
        self.kafka_thread.daemon = True
        self.kafka_thread.start()
        self.indexer = IndexerUtils(config)

    def _kafka_watcher(self):
        c = Consumer({
            'bootstrap.servers': self.server,
            'group.id': self.cgroup,
            'auto.offset.reset': 'earliest'
        })

        c.subscribe([self.topic])

        while True:
            msg = c.poll(0.5)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print("Kafka error: " + msg.error())
                    continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                if data['strcde'] != 'WS':
                    print("Unreconginized strcde")
                    continue
                self.index.process_event(data)
            except BaseException as e:
                print(str(e))
                continue
        c.close()
