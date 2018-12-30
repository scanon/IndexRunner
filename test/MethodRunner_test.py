# -*- coding: utf-8 -*-
import os
import unittest
from unittest.mock import patch
from configparser import ConfigParser

from IndexRunner.MethodRunner import MethodRunner


class MethodRunnerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.token = os.environ.get('KB_AUTH_TOKEN', None)
        config_file = os.environ.get('KB_DEPLOYMENT_CONFIG', None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('IndexRunner'):
            cls.cfg[nameval[0]] = nameval[1]
        # WARNING: don't call any logging metholsds on the context object,
        # it'll result in a NoneType error
        cls.cfg['token'] = cls.token
        cls.mr = MethodRunner(cls.cfg, token=cls.token)

    @patch('IndexRunner.MethodRunner.Catalog', autospec=True)
    def test_run(self, mock_cat):
        params = {'message': 'Hi', 'workspace_name': 'bogus'}
        mr = MethodRunner(self.cfg, self.token)
        mr.catalog.get_module_version.return_value = {'docker_img_name': 'mock_indexer:latest'}
        res = mr.run('kb_GenomeIndexer', 'genome_index', params)
        self.assertIsNotNone(res)
        mr.cleanup()
