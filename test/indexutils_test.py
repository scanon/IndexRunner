# -*- coding: utf-8 -*-
import unittest
import os  # noqa: F401
import json  # noqa: F401
from unittest.mock import patch

from os import environ
from configparser import ConfigParser  # py3
from elasticsearch import Elasticsearch

from pprint import pprint  # noqa: F401

from Workspace.WorkspaceClient import Workspace as workspaceService
from Utils.IndexerUtils import IndexerUtils


class IndexerTester(unittest.TestCase):

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
        cls.upa = '16962/3'
        cls.test_dir = os.path.dirname(os.path.abspath(__file__))
        cls.mock_dir = os.path.join(cls.test_dir, 'mock_data')
        cls.es = Elasticsearch(cls.cfg['elastic-host'])

        cls.wsinfo = cls.read_mock('get_workspace_info.json')
        cls.narobj = cls.read_mock('narrative_object.json')
        cls.genobj = cls.read_mock('get_objects.json')
        cls.new_version_event = {
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
# { "strcde" : "WS", "accgrp" : 10459, "objid" : "31", "ver" : null, "newname" : null, "time" : "2018-02-08T23:55:03.287Z", "evtype" : "NEW_ALL_VERSIONS", "objtype" : null, "objtypever" : null, "public" : false}
# { "strcde" : "WS", "accgrp" : 10459, "objid" : "31", "ver" : null, "newname" : "Escherichia_coli_str_K-12_substr_MG1655_NCBI-deleted-1518134311489", "time" : "2018-02-08T23:58:31.500Z", "evtype" : "RENAME_ALL_VERSIONS", "objtype" : null, "objtypever" : null, "public" : null}
# { "strcde" : "WS", "accgrp" : 10459, "objid" : "31", "ver" : null, "newname" : null, "time" : "2018-02-08T23:58:31.581Z", "evtype" : "DELETE_ALL_VERSIONS", "objtype" : null, "objtypever" : null, "public" : null}
# NEW_VERSION,
# PUBLISH_ALL_VERSIONS,
# PUBLISH_ACCESS_GROUP,
# UNPUBLISH_ALL_VERSIONS,
# UNPUBLISH_ACCESS_GROUP,
# NEW_ALL_VERSIONS,
# RENAME_ALL_VERSIONS,
# DELETE_ALL_VERSIONS,
# UNDELETE_ALL_VERSIONS,
# DELETE_ACCESS_GROUP,
# COPY_ACCESS_GROUP;

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

    @classmethod
    def read_mock(cls, filename):
        with open(os.path.join(cls.mock_dir, filename)) as f:
            obj = json.loads(f.read())
        return obj

    @patch('Utils.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    @patch('Utils.MethodRunner.Catalog', autospec=True)
    def index_object_test(self, mock_wsa, mock_cat):
        iu = IndexerUtils(self.cfg)
        iu.ws.get_workspace_info.return_value = self.wsinfo
        # iu.ws.get_objects2.return_value = {'data': [self.narobj]}
        iu.ws.get_objects2.return_value = self.narobj
        iu.ws.list_objects.return_value = []
        rv = {'docker_img_name': 'mock_indexer:latest'}
        iu.mr.catalog.get_module_version.return_value = rv
        rec = iu._create_obj_rec('1/2/3')
        event = self.new_version_event.copy()
        event['upa'] = '1/2/3'
        res = iu.new_object_version(event)
        self.assertIsNotNone(res)

    @patch('Utils.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    @patch('Utils.MethodRunner.Catalog', autospec=True)
    def index_request_test(self, mock_wsa, mock_cat):
        iu = IndexerUtils(self.cfg)
        iu.ws.get_workspace_info.return_value = self.wsinfo
        iu.ws.get_objects2.return_value = self.genobj
        iu.ws.list_objects.return_value = []
        rv = {'docker_img_name': 'mock_indexer:latest'}
        iu.mr.catalog.get_module_version.return_value = rv
        #mr.catalog.get_module_version.return_value = {'docker_img_name': 'mock_indexer:latest'}
        ev = self.new_version_event.copy()
        ev['objtype'] = 'KBaseGenomes.Genome'
        ev['objid'] = '3'
        id = 'WS:1:3:3'
        try:
            if self.es.indices.exists(index='genome'):
                self.es.delete('genome', 'data', id, routing=id)
                self.es.delete('genomefeature', 'data', id, routing=id)
        except:
            pass
        res = iu.process_event(ev)
        # res = iu.index_request(self.upa)
        # self.assertIsNotNone(res)

    # Test re-index narrative object
    def index_narrative_test(self):
        pass
