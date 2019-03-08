#
# Kafka Event Handler
# This waits for events and dispatches it to the indexer
#
from confluent_kafka import Consumer, KafkaError
import json
from IndexRunner.IndexerUtils import IndexerUtils
import logging


def _log_error(event, error):
    with open('error.log', 'a') as f:
        f.write(str(event)+'\n')
        f.write(str(error)+'\n')


def kafka_watcher(config):
    topic = config.get('kafka-topic', 'wsevents')
    indexer_topic = config.get('kafka-index-topic', 'idxevents')
    server = config.get('kafka-server', 'kafka')
    cgroup = config.get('kafka-clientgroup', 'search_indexer')
    log = logging.getLogger('indexrunner')
    log.info("Initializing EventHandler")
    run_one = 'run_one' in config
    indexer = IndexerUtils(config)
    c = Consumer({
        'bootstrap.servers': server,
        'group.id': cgroup,
        'auto.offset.reset': 'earliest'
    })
    log.info("Starting consumer")
    log.info("Server %s" % (server))
    log.info("Group: %s" % (cgroup))
    log.info("Topic: %s" % (topic))
    log.info("Index Topic: %s" % (indexer_topic))

    c.subscribe([topic, indexer_topic])

    while True:
        msg = c.poll(0.5)

        data = None
        if msg is None:
            pass
        elif msg.error():
            # TODO need comment - why ignore this error?
            if msg.error().code() != KafkaError._PARTITION_EOF:
                err = str(msg.error())
                _log_error('', err)
                log.error("Kafka error: " + err)
        else:
            try:
                data = json.loads(msg.value().decode('utf-8'))
                if data['strcde'] != 'WS':
                    _log_error(data, 'Bad strcde')
                    log.warning("Unreconginized strcde")
                else:
                    indexer.process_event(data)
            except BaseException as e:
                _log_error(data, e)
                log.error('Uncaught exception: ' + str(e))
            # This is just used in testing
        if run_one:
            break
    c.close()
