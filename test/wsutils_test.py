# -*- coding: utf-8 -*-
import unittest
from configparser import ConfigParser
from os import environ

from nose.plugins.attrib import attr

from IndexRunner.WSAdminUtils import WorkspaceAdminUtil
from kbase.Workspace.WorkspaceClient import Workspace as workspaceService


class WSAdminTester(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.token = environ.get('KB_AUTH_TOKEN', None)
        config_file = environ.get('KB_DEPLOYMENT_CONFIG', None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('IndexRunner'):
            cls.cfg[nameval[0]] = nameval[1]
        cls.wsURL = cls.cfg['workspace-url']
        cls.wsClient = workspaceService(cls.wsURL)
        cls.scratch = cls.cfg['scratch']
        cls.cfg['token'] = cls.token
        cls.wsid = 16962

    @attr('online')
    def list_test(self):
        ws = WorkspaceAdminUtil(self.cfg)
        res = ws.list_objects({'ids': [self.wsid]})[0]
        self.assertIsNotNone(res)

    @attr('online')
    def get_objects_test(self):
        ws = WorkspaceAdminUtil(self.cfg)
        ob = ws.list_objects({'ids': [self.wsid]})[0]
        id = '%s/%s' % (self.wsid, ob[0])
        res = ws.get_objects2({'objects': [{'ref': id}]})['data'][0]
        self.assertIsNotNone(res)
