# -*- coding: utf-8 -*-
import json
import os
import unittest
from unittest.mock import patch

from confluent_kafka import KafkaError

from IndexRunner.EventUtils import kafka_watcher


class myerror():
    def __init__(self, code, text):
        self.text = text
        self.ecode = code

    def code(self):
        return self.ecode

    def __str__(self):
        return self.text


class mymessage():
    def __init__(self, msg=None, error_code=None, err_string="error"):
        self.msg = msg
        self.err = None
        if error_code is not None:
            self.err = myerror(error_code, err_string)

    def error(self):
        if self.err is None:
            return False
        return self.err

    def value(self):
        return self.msg


class MethodRunnerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        ev = {
            'strcde': 'WS',
            'wsid': 1,
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
        cls.mock_log = patch('IndexRunner.EventUtils.logging', autospec=True).start()
        cls.mock_in = patch('IndexRunner.EventUtils.IndexerUtils', autospec=True).start()
        cls.mock_con = patch('IndexRunner.EventUtils.Consumer', autospec=True).start()

    def _remove_error_file(self):
        if os.path.exists('error.log'):
            os.remove('error.log')

    def _parse_error(self):
        with open('error.log') as f:
            data = f.readline()
            error = f.readline()
            return data, error

    def _call_watcher(self, msg, called=False, side_effect=None, expected_error=None):
        self._remove_error_file()
        self.mock_con.return_value.poll.return_value = msg
        if side_effect:
            self.mock_in.return_value.process_event.side_effect = side_effect

        self.mock_in.return_value.process_event.reset_mock()
        kafka_watcher({'run_one': 1})
        if called:
            self.mock_in.return_value.process_event.assert_called_once()
        else:
            self.mock_in.return_value.process_event.assert_not_called()

        if expected_error is None:
            self.assertFalse(os.path.exists('error.log'))
        else:
            self.assertTrue(os.path.exists('error.log'))
            data, err = self._parse_error()
            self.assertIn(expected_error, err)

    def test_empty_message(self):
        self._call_watcher(None, called=False)

    def test_good_case(self):
        self._call_watcher(mymessage(self.ev), called=True)

    def test_bad_string_code(self):
        self._call_watcher(mymessage(self.badev), called=False,
                           expected_error='Bad strcde')

    def test_bad_json(self):
        self._call_watcher(mymessage('blah'.encode()), called=False,
                           expected_error='Expecting value')

    def test_index_exception(self):
        self._call_watcher(mymessage(self.ev), side_effect=Exception('bogus'),
                           called=True, expected_error='bogus')

    def test_kafka_error(self):
        msg = mymessage(self.ev, 1, 'bad kafka, bad')
        self._call_watcher(msg, side_effect=Exception('bogus'),
                           called=False, expected_error='bad kafka')

    def test_kafka_partition_error(self):
        msg = mymessage(self.ev, KafkaError._PARTITION_EOF, 'ignore this')
        self._call_watcher(msg, side_effect=Exception('bogus'), called=False)
