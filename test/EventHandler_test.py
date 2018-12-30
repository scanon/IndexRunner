# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch
import json
from IndexRunner.EventUtils import kafka_watcher
from confluent_kafka import KafkaError
import os


class mymessage():
    def __init__(self, msg=None, error=False):
        self.msg = msg
        self.ev = error

    def error(self):
        return self.ev

    def value(self):
        return self.msg


class MethodRunnerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        ev = {
            'strcde': 'WS',
            'accgrp': 1,
            'objid': '2',
            'ver': 3,
            'newname': None,
            'evtype': 'NEW_VERSION',
            'time': '2018-02-08T23:23:25.553Z',
            'objtype': 'KBaseNarrative.Narrative',
            'objtypever': 4,
            'public': False
            }
        cls.ev = json.dumps(ev).encode()
        ev['strcde'] = 'BOGUS'
        cls.badev = json.dumps(ev).encode()

    def _remove_error_file(self):
        if os.path.exists('error.log'):
            os.remove('error.log')

    def _parse_error(self):
        with open('error.log') as f:
            data = f.readline()
            error = f.readline()
            return data, error

    @patch('IndexRunner.EventUtils.Consumer', autospec=True)
    @patch('IndexRunner.EventUtils.IndexerUtils', autospec=True)
    @patch('IndexRunner.EventUtils.logging', autospec=True)
    def test_watcher(self, mock_log, mock_in, mock_con):
        # Test an empty message
        self._remove_error_file()
        mock_con.return_value.poll.return_value = None
        kafka_watcher({'run_one': 1})
        mock_in.return_value.process_event.assert_not_called()
        self.assertFalse(os.path.exists('error.log'))

        # Test the good case
        msg = mymessage(self.ev)
        mock_con.return_value.poll.return_value = msg
        kafka_watcher({'run_one': 1})
        mock_in.return_value.process_event.assert_called_once()
        self.assertFalse(os.path.exists('error.log'))

        # Bad string code
        self._remove_error_file()
        msg = mymessage(self.badev)
        mock_con.return_value.poll.return_value = msg
        mock_in.return_value.process_event.reset_mock()
        kafka_watcher({'run_one': 1})
        mock_in.return_value.process_event.assert_not_called()
        self.assertTrue(os.path.exists('error.log'))
        data, err = self._parse_error()
        self.assertIn('Bad strcde', err)

        # Test bad json
        self._remove_error_file()
        msg = mymessage('blah'.encode())
        mock_con.return_value.poll.return_value = msg
        mock_in.return_value.process_event.reset_mock()
        kafka_watcher({'run_one': 1})
        mock_in.return_value.process_event.assert_not_called()
        self.assertTrue(os.path.exists('error.log'))
        data, err = self._parse_error()
        self.assertIn('Expecting value', err)

        # Test index exception
        self._remove_error_file()
        msg = mymessage(self.ev)
        mock_con.return_value.poll.return_value = msg
        mock_in.return_value.process_event.side_effect = Exception('bogus')
        mock_in.return_value.process_event.reset_mock()
        kafka_watcher({'run_one': 1})
        mock_in.return_value.process_event.assert_called_once()
        self.assertTrue(os.path.exists('error.log'))
        data, err = self._parse_error()
        self.assertIn('bogus', err)

        # Kafka Error
        self._remove_error_file()
        msg = mymessage(self.ev, 'bad kafka, bad')
        mock_con.return_value.poll.return_value = msg
        mock_in.return_value.process_event.side_effect = Exception('bogus')
        mock_in.return_value.process_event.reset_mock()
        kafka_watcher({'run_one': 1})
        mock_in.return_value.process_event.assert_not_called()
        self.assertTrue(os.path.exists('error.log'))
        data, err = self._parse_error()
        self.assertIn('bad kafka', err)
