# -*- coding: utf-8 -*-
import unittest
import os  # noqa: F401
import json  # noqa: F401
import time
from IndexRunner.WSAdminUtils import WorkspaceAdminUtil

from os import environ
from configparser import ConfigParser  # py3

from pprint import pprint  # noqa: F401

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
        # Getting username from Auth profile for token
        # authServiceUrl = cls.cfg['auth-service-url']
        # auth_client = _KBaseAuth(authServiceUrl)
        # user_id = auth_client.get_user(cls.token)
        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        cls.wsURL = cls.cfg['workspace-url']
        cls.wsClient = workspaceService(cls.wsURL)
        cls.scratch = cls.cfg['scratch']
        cls.cfg['token'] = cls.token
        cls.wsid = 16962

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

    def getWsClient(self):
        return self.__class__.wsClient

    def getWsName(self):
        if hasattr(self.__class__, 'wsName'):
            return self.__class__.wsName
        suffix = int(time.time() * 1000)
        wsName = "test_NarrativeIndexer_" + str(suffix)
        ret = self.getWsClient().create_workspace({'workspace': wsName})  # noqa
        self.__class__.wsName = wsName
        return wsName

    def list_test(self):
        ws = WorkspaceAdminUtil(self.cfg)
        res = ws.list_objects({'ids': [self.wsid]})[0]
        self.assertIsNotNone(res)

    def get_objects_test(self):
        ws = WorkspaceAdminUtil(self.cfg)
        ob = ws.list_objects({'ids': [self.wsid]})[0]
        id = '%s/%s' % (self.wsid, ob[0])
        res = ws.get_objects2({'objects': [{'ref': id}]})['data'][0]
        self.assertIsNotNone(res)
