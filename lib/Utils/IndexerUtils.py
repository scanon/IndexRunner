from Utils.WSAdminUtils import WorkspaceAdminUtil
from Utils.MethodRunner import MethodRunner
from elasticsearch import Elasticsearch

from time import time
import json
import yaml

# This is the interface that will handle the event

# Type Mappings


class IndexerUtils:

    def __init__(self, config):
        self.ws = WorkspaceAdminUtil(config)
        self.fakeid = 99999
        self.fakever = 1
        mapfile = config.get('mapping-file')
        with open(mapfile) as f:
            d = f.read()
            self.mapping = yaml.load(d)['types']

            self.es = Elasticsearch([config['elastic-host']])
        self.esbase = config['elastic-base']
        self.mr = MethodRunner(config, token=config['ws-admin-token'])
        with open('specs/mapping.json') as f:
            d = f.read()
            self.mapping_spec = json.loads(d)

    def process_event(self, event):
        # Sample record...
        # { "strcde" : "WS",
        #   "accgrp" : 10459,
        #   "objid" : "1",
        #   "ver" : 227,
        #   "newname" : null,
        #   "time" : "2018-02-08T23:23:25.553Z",
        #   "evtype" : "NEW_VERSION",
        #   "objtype" : "KBaseNarrative.Narrative",
        #   "objtypever" : 4,
        #   "public" : false}

        etype = event['evtype']
        event['upa'] = '%d/%s/%d' % (event['accgrp'], event['objid'], event['ver'])
        if etype in ['NEW_VERSION', 'NEW_ALL_VERSIONS']:
            print("New version")
            self.new_object_version(event)
        elif etype in ['NEW_ALL_VERSIONS', 'RENAME_ALL_VERSIONS', 'UNDELETE_ALL_VERSIONS']:
            print("All versions")
        elif etype in ['REINDEX_WORKSPACE']:
            print("reindex")
        elif etype.find('DELETE_') >= 0 or etype.find('PUBLISH_') >= 0:
            self.update_access(event['accgrp'])
        elif etype.find('_ACCESS_GROUP') > 0:
            # Change in publish state
            print("Warning ACCESS GROUP not handled")
        else:
            print("Can't process evtype " + event['evtype'])

    def _create_obj_rec(self, upa):
        (wsid, objid, vers) = self._split_upa(upa)
        obj = self.ws.get_objects2({'objects': [{'ref': upa}]})['data'][0]

        wsinfo = self._get_ws_info(wsid)
        # Don't index temporary narratives
        if wsinfo['temp']:
            return None

        prov = self._get_prov(obj)

        oinfo = obj['info']

        rec = {
          "guid": "WS:%s" % (upa),
          "otype": None,
          "otypever": 1,
          "stags": [],
          "oname": oinfo[1],
          "creator": oinfo[5],
          "copier": None,
          "prv_mod": prov['prv_mod'],
          "prv_meth": prov['prv_meth'],
          "prv_ver": prov['prv_ver'],
          "prv_cmt": None,
          "md5": oinfo[10].get('MD5'),
          "timestamp": int(time()),
          "prefix": "WS:%d/%d" % (wsid, objid),
          "str_cde": "WS",
          "accgrp": wsid,
          "version": vers,
          "islast": False,
          "public": wsinfo['public'],
          "shared": wsinfo['shared'],
          "ojson": "{}",
          "pjson": None
          }

        return rec

    def _get_upa(self, obj):
        return '%s/%s/%s' % (obj[6], obj[0], obj[4])

    def _access_rec(self, wsid, objid, vers, public=False):
        rec = {
            "extpub": [],
            "groups": [
                -2,
                wsid
            ],
            "lastin": [
                -2,
                wsid
            ],
            "pguid": "WS:%s/%s/%s" % (wsid, objid, vers),
            "prefix": "WS:%s/%s" % (wsid, objid),
            "version": vers
        }
        if public:
            rec['lastin'].append(-1)
            rec['groups'].append(-1)
        # type": "access"
        return rec

    def _get_wsid(self, upa):
        """
        Return the workspace id as an int from an UPA
        """
        return int(str(upa).split('/')[0])

    def _get_id(self, upa):
        """
        Return the elastic id
        """
        if upa.find("/") < 0:
            raise ValueError("Not an upa")
        return "WS:%s" % (upa.replace('/', ':'))

    def _get_prov(self, obj):
        prov = obj['provenance'][0]
        ret = {
          "prv_mod": None,
          "prv_meth": None,
          "prv_ver": None,
          "prv_cmt": None,

        }
        if 'service' in prov:
            ret['prv_mod'] = prov['service']

        if 'method' in prov:
            ret['prv_meth'] = prov['method']
        if 'script' in prov:
            ret['prv_mod'] = 'legacy_transform'
            ret['prv_meth'] = prov['script']

        if 'service_ver' in prov:
            ret['prv_ver'] = prov['service_ver']
        elif 'script_ver' in prov:
            ret['prv_ver'] = prov['script_ver']

        if 'description' in prov:
            ret['prv_cmt'] = prov['description']
        # print(ret)
        return ret

    def _get_es_data_record(self, index, upa):
        eid = self._get_id(upa)
        try:
            res = self.es.get(index=index, routing=eid, doc_type='data', id=eid)
        except:
            return None
        return res

    def _put_es_data_record(self, index, upa, doc, version=None, reindex=False):
        eid = self._get_id(upa)
        if reindex:
            res = self.es.index(index=index, parent=eid, doc_type='data',
                                id=eid, routing=eid, body=doc)
        elif version is None:
            res = self.es.create(index=index, parent=eid, doc_type='data',
                                 id=eid, routing=eid, body=doc)
        else:
            res = self.es.index(index=index, parent=eid, doc_type='data',
                                id=eid, routing=eid, version=version, body=doc)
        return res

    def _get_ws_info(self, wsid):
        info = self.ws.get_workspace_info({'id': wsid})
        meta = info[8]
        # Don't index temporary narratives
        temp = False
        if meta.get('is_temporary') == 'true':
            temp = True

        public = False
        if info[6] != 'n':
            public = True

        # TODO
        shared = False

        return {'wsid': wsid, 'info': info, 'meta': meta,
                'temp': temp, 'public': public, 'shared': shared}

    def update_access(self, upa):
        # Find each index
        for index in ['todo']:
            self._update_es_access(index, upa)

    def _update_es_access(self, index, wsid, objid, vers, upa):
        # Should pass a wsid but just in case...
        wsinfo = self._get_ws_info(wsid)
        if wsinfo['temp']:
            return None
        public = wsinfo['public']
        doc = self._access_rec(wsid, objid, vers, public=public)
        eid = self._get_id(upa)
        res = self.es.index(index=index, doc_type='access', id=eid, body=doc)
        return res

    def _split_upa(self, upa):
        return map(lambda x: int(x), upa.split('/'))

    def _get_indexes(self, otype):
        if otype in self.mapping:
            return self.mapping[otype]
        return self.mapping['Other']

    def _check_mapping(self, oindex):
        index = oindex['index_name']
        res = self.es.indices.exists(index=index)
        if not res:
            (module, method) = oindex['mapping_method'].split('.')
            params = {}
            extra = self.mr.run(module, method, params)
            schema = self.mapping_spec
            schema['key'] = {'properties': extra}
            self.es.indices.create(index=index, body=schema)

    def _new_object_version_index(self, event, oindex):
        wsid = event['accgrp']
        objid = event['objid']
        vers = event['ver']
        upa = event['upa']
        index = oindex['index_name']
        self._check_mapping(oindex)

        rec = self._get_es_data_record(index, upa)
        # TODO check if WS is deleted
        if rec is not None:
            return

        doc = self._create_obj_rec(upa)
        params = {'upa': upa}
        (module, method) = oindex['index_method'].split('.')
        extra = self.mr.run(module, method, params)
        if 'data' in extra and extra['data'] is not None:
            doc['keys'] = extra['data']
        doc['ojson'] = json.dumps(doc['keys'])
        self._update_es_access(index, wsid, objid, vers, upa)
        self._put_es_data_record(index, upa, doc)

    def _new_object_version_feature_index(self, event, oindex):
        wsid = event['accgrp']
        objid = event['objid']
        vers = event['ver']
        upa = event['upa']
        index = oindex['index_name']
        self._check_mapping(oindex)

        # Check if any exists
        rec = None
        # rec = self._get_es_data_record(index, upa)
        if rec is not None:
            return

        doc = self._create_obj_rec(upa)
        params = {'upa': upa}
        (module, method) = oindex['index_method'].split('.')
        extra = self.mr.run(module, method, params)
        parent = extra['parent']
        if 'features' in extra and extra['features'] is not None:
            features = extra['features']
        doc['keys'] = parent
        doc['ojson'] = json.dumps(doc['keys'])
        #self._update_es_access(index, wsid, objid, vers, upa)
        #self._put_es_data_record(index, upa, doc)

    def new_object_version(self, event):
        indexes = self._get_indexes(event['objtype'])
        for oindex in indexes:
            try:
                if 'feature' in oindex and oindex['feature']:
                    self._new_object_version_feature_index(event, oindex)
                else:
                    self._new_object_version_index(event, oindex)
            except:
                raise
                print("Failed for index")
        return True
