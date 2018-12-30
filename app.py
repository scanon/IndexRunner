import os
from Utils.EventUtils import kafka_watcher
from configparser import ConfigParser
import time
import logging
from threading import Thread

# create logger
logger = logging.getLogger('indexrunner')

# Set Level
log_level = logging.INFO
if 'LOG_LEVEL' in os.environ:
    log_level = getattr(logging, os.environ['LOG_LEVEL'].upper())

logger.setLevel(log_level)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(log_level)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

config_file = os.environ.get('KB_DEPLOYMENT_CONFIG', None)
config = ConfigParser()
config.read(config_file)
cfg = dict()
for nameval in config.items('IndexRunner'):
    cfg[nameval[0]] = nameval[1]

kafka_thread = Thread(target=kafka_watcher, args=[cfg])
kafka_thread.daemon = True
kafka_thread.start()
while True:
    time.sleep(600)
