# -*- coding: utf-8 -*-
import datetime
import json  # noqa: F401
import os  # noqa: F401
import unittest
from configparser import ConfigParser
from os import environ
from unittest.mock import patch, Mock

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from nose.plugins.attrib import attr

from IndexRunner.IndexerUtils import IndexerUtils


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
        cls.scratch = cls.cfg['scratch']
        cls.cfg['token'] = cls.token
        cls.cfg['kafka-server'] = None
        cls.base = cls.cfg['elastic-base']
        cls.test_dir = os.path.dirname(os.path.abspath(__file__))
        cls.mock_dir = os.path.join(cls.test_dir, 'mock_data')
        cls.es = Elasticsearch(cls.cfg['elastic-host'])

        cls.wsinfo = cls.read_mock('get_workspace_info.json')
        cls.wslist = cls.read_mock('list_objects.json')
        cls.narobj = cls.read_mock('narrative_object.json')
        cls.genobj = cls.read_mock('genome_object_nodata.json')
        cls.geninfo = cls.read_mock('genome_object_info.json')
        cls.new_version_event = {
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

    @classmethod
    def read_mock(cls, filename):
        with open(os.path.join(cls.mock_dir, filename)) as f:
            obj = json.loads(f.read())
        return obj

    def reset(self):
        for i in ['genome', 'genomefeature', 'objects', 'sketch']:
            try:
                if self.es.indices.exists(index=self._iname(i)):
                    self.es.indices.delete(index=self._iname(i))
            except:
                pass

    def _iname(self, index):
        return '%s.%s' % (self.base, index)

    def _init_es_genome(self):
        self.reset()
        with open(os.path.join(self.mock_dir, 'genome-es-map.json')) as f:
            d = json.loads(f.read())

        indices = d.keys()
        for index in indices:
            schema = d[index]
            schema.pop('settings')
            self.es.indices.create(index=self._iname(index), body=schema)
        with open(os.path.join(self.mock_dir, 'genome-es.json')) as f:
            d = json.loads(f.read())['hits']['hits']
        for r in d:
            i = r['_index']
            r['_index'] = self._iname(i)
        bulk(self.es, d)
        for index in indices:
            self.es.indices.refresh(index=self._iname(index))

    def get_indexes_test(self):
        iu = IndexerUtils(self.cfg)
        self.assertEqual(iu._get_indexes("KBaseNarrative.Narrative"),
                         [{'index_method': 'NarrativeIndexer.index',
                           'index_name': 'ciraw.narrative'}]
                         )
        self.assertEqual(iu._get_indexes("Foo.Bar"),
                         [{'comment': 'Everything without a specific indexer',
                           'index_method': 'default_indexer',
                           'index_name': 'ciraw.objects'}])
        self.assertEqual(iu._get_indexes("KBaseMatrices.ExpressionMatrix"),
                         [{'index_method': 'KBaseMatrices.index',
                           'index_name': 'ciraw.KBaseMatrices'}])

    @patch('IndexRunner.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    def skip_temp_test(self, wsa):
        iu = IndexerUtils(self.cfg)
        wsi = self.wsinfo
        wsi[8]['is_temporary'] = 'true'
        iu.ws.get_workspace_info.return_value = wsi
        # iu.ws.get_objects2.return_value = {'data': [self.narobj]}
        iu.ws.get_objects2.return_value = self.narobj
        res = iu._create_obj_rec('1/2/3')
        self.assertIsNone(res)
        res = iu._update_es_access('bogus', 1, 2, 3, '1/2/3')
        self.assertIsNone(res)

    def prov_test(self):
        iu = IndexerUtils(self.cfg)
        obj = dict()
        resp = iu._get_prov(obj)
        self.assertIn('prv_meth', resp)
        self.assertIsNone(resp['prv_meth'])
        prv = {
            'service': 'bogus',
            'method': 'bogus2',
            'service_ver': '1.0',
            'description': 'desc'
            }
        obj['provenance'] = [prv]
        resp = iu._get_prov(obj)
        self.assertEquals(resp['prv_mod'], 'bogus')
        self.assertEquals(resp['prv_meth'], 'bogus2')
        self.assertEquals(resp['prv_ver'], '1.0')
        self.assertEquals(resp['prv_cmt'], 'desc')

        # Test script
        obj['provenance'] = [{'script': 'bogus3', 'script_ver': '1.1'}]
        resp = iu._get_prov(obj)
        self.assertEquals(resp['prv_mod'], 'legacy_transform')
        self.assertEquals(resp['prv_meth'], 'bogus3')
        self.assertEquals(resp['prv_ver'], '1.1')

    def acces_rec_test(self):
        iu = IndexerUtils(self.cfg)
        resp = iu._access_rec(1, 2, 3, public=True)
        self.assertIn(-1, resp['groups'])

    def get_id_test(self):
        iu = IndexerUtils(self.cfg)
        with self.assertRaises(ValueError):
            iu._get_id('blah')

    @patch('IndexRunner.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    @patch('IndexRunner.MethodRunner.Catalog', autospec=True)
    def index_object_test(self, mock_wsa, mock_cat):
        iu = IndexerUtils(self.cfg)
        iu.ws.get_workspace_info.return_value = self.wsinfo
        # iu.ws.get_objects2.return_value = {'data': [self.narobj]}
        iu.ws.get_objects2.return_value = self.narobj
        rv = {'docker_img_name': 'mock_indexer:latest'}
        iu.method_runner.catalog.get_module_version.return_value = rv
        event = self.new_version_event.copy()
        event['upa'] = '1/2/3'
        res = iu.new_object_version(event)
        self.assertIsNotNone(res)

    @patch('IndexRunner.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    @patch('IndexRunner.MethodRunner.Catalog', autospec=True)
    def index_request_test(self, mock_wsa, mock_cat):
        iu = IndexerUtils(self.cfg)
        iu.ws.get_workspace_info.return_value = self.wsinfo
        iu.ws.get_objects2.return_value = self.genobj
        iu.ws.get_object_info3.return_value = self.geninfo
        rv = {'docker_img_name': 'mock_indexer:latest'}
        iu.method_runner.catalog.get_module_version.return_value = rv
        ev = self.new_version_event.copy()
        ev['objtype'] = 'KBaseGenomes.Genome'
        ev['objid'] = '3'
        self.reset()
        iu.process_event(ev)
        id = 'WS:1:3:3'
        res = self.es.get(index=self._iname('genome'), routing=id, doc_type='data', id=id)
        self.assertIsNotNone(res)
        self.assertTrue(res['_source']['islast'])

        # Test with older version
        ev = self.new_version_event.copy()
        ev['objtype'] = 'KBaseGenomes.Genome'
        ev['objid'] = '3'
        ev['ver'] = 2
        self.reset()
        iu.process_event(ev)
        id = 'WS:1:3:2'
        res = self.es.get(index=self._iname('genome'), routing=id, doc_type='data', id=id)
        self.assertIsNotNone(res)
        self.assertFalse(res['_source']['islast'])

    @patch('IndexRunner.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    @patch('IndexRunner.MethodRunner.Catalog', autospec=True)
    def index_request_default_test(self, mock_wsa, mock_cat):
        iu = IndexerUtils(self.cfg)
        iu.ws.get_workspace_info.return_value = self.wsinfo
        obj = self.genobj
        iu.ws.get_objects2.return_value = obj
        iu.ws.get_object_info3.return_value = self.geninfo
        ev = self.new_version_event.copy()
        ev['objtype'] = 'Blah.Blah'
        ev['objid'] = '3'
        id = 'WS:1:3:3'
        self.reset()
        iu.process_event(ev)
        res = self.es.get(index=self._iname('objects'), routing=id, doc_type='data', id=id)
        self.assertIsNotNone(res)
        self.assertTrue(res['_source']['islast'])

    @patch('IndexRunner.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    @patch('IndexRunner.MethodRunner.Catalog', autospec=True)
    def index_new_all_test(self, mock_wsa, mock_cat):
        iu = IndexerUtils(self.cfg)
        iu.ws.get_workspace_info.return_value = self.wsinfo
        iu.ws.get_objects2.return_value = self.genobj
        iu.ws.get_object_info3.return_value = self.geninfo
        rv = {'docker_img_name': 'mock_indexer:latest'}
        iu.method_runner.catalog.get_module_version.return_value = rv
        ev = self.new_version_event.copy()
        ev['objtype'] = None
        ev['objid'] = '3'
        ev['evtype'] = 'NEW_ALL_VERSIONS'
        ev['ver'] = None
        id = 'WS:1:3:3'
        self.reset()
        iu.process_event(ev)
        res = self.es.get(index=self._iname('genome'), routing=id, doc_type='data', id=id)
        self.assertIsNotNone(res)
        self.assertIn('ojson', res['_source'])
        self.assertTrue(res['_source']['islast'])
        self.assertNotIn('objdata', res['_source'])

    @attr('online')
    @patch('IndexRunner.MethodRunner.Catalog', autospec=True)
    def index_request_genome_test(self, mock_cat):
        iu = IndexerUtils(self.cfg)
        rv = {'docker_img_name': 'test/kb_genomeindexer:latest'}
        iu.method_runner.catalog.get_module_version.return_value = rv
        ev = self.new_version_event.copy()
        wsid = 15792
        objid = 2
        ver = 12
        ev['objtype'] = 'KBaseGenomes.Genome'
        ev['objid'] = str(objid)
        ev['wsid'] = wsid
        ev['ver'] = ver
        id = f'WS:{wsid:d}:{objid:d}:{ver:d}'
        self.reset()
        iu.process_event(ev)
        res = self.es.get(index=self._iname('genome'), routing=id, doc_type='data', id=id)
        self.assertIsNotNone(res)
        fid = f'WS:{wsid:d}/{objid:d}/{ver:d}:feature/L876_RS0116375'
        res = self.es.get(index=self._iname('genomefeature'), routing=id,
                          doc_type='data', id=fid)
        self.assertIsNotNone(res)
        self.assertIn('ojson', res['_source'])
        self.assertTrue(res['_source']['islast'])

    def index_error_test(self):
        iu = IndexerUtils(self.cfg)
        if os.path.exists('error.log'):
            os.remove('error.log')
        iu._new_object_version_index = Mock(side_effect=KeyError())
        iu._new_object_version_feature_index = Mock(side_effect=KeyError())
        ev = self.new_version_event.copy()
        iu.process_event(ev)
        self.assertTrue(os.path.exists('error.log'))

    @patch('IndexRunner.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    def publish_test(self, ws_mock):
        """
        Publish and unpublish tests
        """
        # PUBLISH_ALL_VERSIONS,
        # PUBLISH_ACCESS_GROUP,
        # UNPUBLISH_ALL_VERSIONS,
        # UNPUBLISH_ACCESS_GROUP,
        mwsi = [1,
                "auser:narrative_1485560571814",
                "auser",
                "2018-10-18T00:12:42+0000",
                25,
                "a",
                "y",
                "unlocked",
                {"narrative_nice_name": "A Fancy Nasrrative",
                 "is_temporary": "false",
                 "data_palette_id": "22", "narrative": "23"
                 }
                ]

        self._init_es_genome()
        ev = {
            "strcde": "WS",
            "wsid": 1,
            "objid": None,
            "ver": None,
            "newname": None,
            "time": datetime.datetime.utcnow(),
            "evtype": "PUBLISH_ACCESS_GROUP",
            "objtype": None,
            "objtypever": None,
            "public": None
        }
        iu = IndexerUtils(self.cfg)
        iu.ws.get_workspace_info.return_value = mwsi
        iu.process_event(ev)
        # Check that accgrp changed
        id = 'WS:1:3:3'
        res = self.es.get(index=self._iname('genome'), routing=id, doc_type='access',
                          id=id)
        self.assertIn(-1, res['_source']['groups'])
        res = self.es.get(index=self._iname('genome'), routing=id, doc_type='data',
                          id=id)
        self.assertTrue(res['_source']['public'])
        #
        ev['evtype'] = "UNPUBLISH_ACCESS_GROUP"
        mwsi[6] = 'n'
        iu = IndexerUtils(self.cfg)
        iu.ws.get_workspace_info.return_value = mwsi
        iu.process_event(ev)
        # Check that accgrp changed
        id = 'WS:1:3:3'
        res = self.es.get(index=self._iname('genome'), routing=id,
                          doc_type='access', id=id)
        self.assertNotIn(-1, res['_source']['groups'])
        res = self.es.get(index=self._iname('genome'), routing=id,
                          doc_type='data', id=id)
        self.assertFalse(res['_source']['public'])

    @patch('IndexRunner.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    def delete_event_test(self, mock_ws):
        # DELETE_ALL_VERSIONS,
        # UNDELETE_ALL_VERSIONS,
        # DELETE_ACCESS_GROUP,
        self._init_es_genome()
        ev = self.new_version_event.copy()
        ev['evtype'] = 'DELETE_ALL_VERSIONS'
        ev['objid'] = '3'
        iu = IndexerUtils(self.cfg)
        iu.process_event(ev)
        id = 'WS:1:3:3'
        res = self.es.get(index=self._iname('genome'), routing=id, doc_type='data',
                          id=id, ignore=404)
        self.assertFalse(res['found'])
        #
        ev = self.new_version_event.copy()
        ev['evtype'] = 'UNDELETE_ALL_VERSIONS'
        iu = IndexerUtils(self.cfg)
        # iu.process_event(ev)

    def rename_event_test(self):
        # RENAME_ALL_VERSIONS,
        ev = self.new_version_event.copy()
        ev['evtype'] = 'RENAME_ALL_VERSIONS'
        ev['objid'] = '3'
        iu = IndexerUtils(self.cfg)
        iu.process_event(ev)
        # TODO

    def bogus_event_test(self):
        # RENAME_ALL_VERSIONS,
        ev = self.new_version_event.copy()
        ev['evtype'] = 'BOGUS'
        iu = IndexerUtils(self.cfg)
        iu.process_event(ev)
        # Basically make sure this doesn't fail with an error

    @patch('IndexRunner.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    @patch('IndexRunner.IndexerUtils.EventProducer', autospec=True)
    def copy_event_test(self, mock_ep, mock_ws):
        # COPY_ACCESS_GROUP;
        ev = {
            "strcde": "WS",
            "wsid": 1,
            "objid": None,
            "ver": None,
            "newname": None,
            "time": datetime.datetime.utcnow(),
            "evtype": "COPY_ACCESS_GROUP",
            "objtype": None,
            "objtypever": None,
            "public": False
        }
        iu = IndexerUtils(self.cfg)
        iu.ws.list_objects.return_value = self.wslist
        iu.process_event(ev)
        iu.ep.index_objects.assert_called()

    @patch('IndexRunner.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    @patch('IndexRunner.IndexerUtils.EventProducer', autospec=True)
    def reindex_event_test(self, mock_ep, mock_ws):
        ev = {
            "strcde": "WS",
            "wsid": 1,
            "objid": None,
            "ver": None,
            "newname": None,
            "time": datetime.datetime.utcnow(),
            "evtype": "REINDEX_WORKSPACE",
            "objtype": None,
            "objtypever": None,
            "public": False
        }
        iu = IndexerUtils(self.cfg)
        iu.ws.list_objects.return_value = self.wslist
        iu.process_event(ev)
        iu.ep.index_objects.assert_called()

    @patch('IndexRunner.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    @patch('IndexRunner.IndexerUtils.MethodRunner', autospec=True)
    def index_raw_test(self, mock_wsa, mock_cat):
        iu = IndexerUtils(self.cfg)
        iu.mapping['KBaseGenomes.Genome'] = [
            {
                'index_method': 'bogus.genome_index',
                'raw': True,
                'index_name': 'ciraw.sketch'
            }
        ]
        index = {
            'schema': {
                'upa': {'type': 'string'},
                'mash': {'type': 'binary'},
                'timestamp': {'type': 'long'}
            },
            'data': {
                'upa': '1/3/2',
                'mash': 'blaslsadlfasdf',
                'timestamp': '1234'
            }
        }
        iu.method_runner.run.return_value = [index]
        ev = self.new_version_event.copy()
        ev['objtype'] = 'KBaseGenomes.Genome'
        ev['objid'] = '3'
        ev['ver'] = 2
        self.reset()
        iu.process_event(ev)
        id = "WS:1:3:2"
        res = self.es.get(index=self._iname('sketch'), doc_type='data', id=id)
        self.assertIsNotNone(res)
        self.assertIn('mash', res['_source'])
        # Code path test... index and indexed object
        iu.process_event(ev)
        # Code path test ignore empty results
        ev['objtype'] = 'KBaseGenomes.Genome'
        ev['objid'] = '4'
        ev['ver'] = 2
        iu.method_runner.run.return_value = [{}]
        iu.process_event(ev)
        # This will throw an error

    @attr('online')
    def index_request_misc_test(self):
        iu = IndexerUtils(self.cfg)
        ev = self.new_version_event.copy()
        ev['objtype'] = 'KBaseGenomeAnnotations.Assembly'
        ev['objid'] = '31'
        ev['accgrp'] = 7998
        ev['ver'] = 2
        id = 'WS:7998:31:2'
        self.reset()
        iu.process_event(ev)
        res = self.es.get(index=self._iname('assembly'), routing=id, doc_type='data', id=id)
        self.assertIsNotNone(res)
        # fid = 'WS:15792:2:1:L876_RS0116375'
        # res = self.es.get(index=self._iname('assemblycontig'), routing=id,
        #                   doc_type='data', id=fid)
        # self.assertIsNotNone(res)
        # self.assertIn('ojson', res['_source'])
        # self.assertTrue(res['_source']['islast'])
