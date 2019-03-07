# -*- coding: utf-8 -*-
import json
import os
import unittest
from unittest.mock import patch

from IndexRunner.EventProducer import EventProducer


class EventProducerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_dir = os.path.dirname(os.path.abspath(__file__))
        cls.mock_dir = os.path.join(cls.test_dir, 'mock_data')
        with open(cls.mock_dir + '/list_objects.json') as f:
            d = f.read()
        cls.objects = json.loads(d)

    @patch('IndexRunner.EventProducer.Producer', autospec=True)
    def test_producer(self, mock_prod):
        ep = EventProducer({})
        ep.index_objects(self.objects)
        ep.prod.produce.assert_called()
