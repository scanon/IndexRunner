from IndexRunner.WSAdminUtils import WorkspaceAdminUtil
from IndexRunner.MethodRunner import MethodRunner
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import bulk
import os
import sys
import traceback

from time import time
import json
import yaml
import logging

# This is the interface that will process indexing
BULK_MAX = 9


class IndexerUtils:
    add_template = """
        if (ctx._source.lastin.indexOf({grp}) < 0) {{
            ctx._source.lastin.add({grp});
        }}
        if (ctx._source.groups.indexOf({grp}) < 0) {{
          ctx._source.groups.add({grp});
        }}
        """

    del_template = """
        if (ctx._source.lastin.indexOf({grp}) >= 0) {{
          ctx._source.lastin.remove(ctx._source.lastin.indexOf({grp}));
        }}
        if (ctx._source.groups.indexOf({grp}) >= 0) {{
          ctx._source.groups.remove(ctx._source.groups.indexOf({grp}));
        }}
        """

    def __init__(self, config):
        self.ws = WorkspaceAdminUtil(config)
        mapfile = config.get('mapping-file')
        with open(mapfile) as f:
            d = f.read()
            self.mapping = yaml.load(d)['types']

            self.es = Elasticsearch([config['elastic-host']])
        self.esbase = config['elastic-base']
        if 'workspace-admin-token' in config:
            token = config['workspace-admin-token']
        else:
            token = os.environ.get('KB_AUTH_TOKEN')
        self.mr = MethodRunner(config, token=token)
        with open('specs/mapping.json') as f:
            d = f.read()
            self.mapping_spec = json.loads(d)

        self.log = logging.getLogger('indexrunner')

    def process_event(self, evt):

        etype = evt['evtype']
        if evt['ver']:
            evt['upa'] = '%d/%s/%d' % (evt['accgrp'], evt['objid'], evt['ver'])
        if etype in ['NEW_VERSION', 'NEW_ALL_VERSIONS']:
            self.new_object_version(evt)
        elif 'PUBLISH' in etype:
            self.publish(evt['accgrp'])
        elif etype.startswith('DELETE_'):
            self.delete(evt)
        elif etype == 'COPY_ACCESS_GROUP':
            self.log.warning("Warning copy not implemented.")
        elif etype == 'RENAME_ALL_VERSIONS':
            self.log.warning("Warning rename not implemented.")
        elif etype in ['REINDEX_WORKSPACE']:
            # Pseudo event
            self.log.warning("TODO reindex")
        else:
            self.log.error("Can't process evtype " + evt['evtype'])

    def _create_obj_rec(self, upa):
        (wsid, objid, vers) = self._split_upa(upa)
        req = {'objects': [{'ref': upa}], 'no_data': 1}
        obj = self.ws.get_objects2(req)['data'][0]
        info = obj['info']

        wsinfo = self._get_ws_info(wsid)
        # Don't index temporary narratives
        if wsinfo['temp']:
            return None

        prov = self._get_prov(obj)

        rec = {
          "guid": "WS:%s" % (upa),
          "otype": None,
          "otypever": 1,
          "stags": [],
          "oname": info[1],
          "creator": info[5],
          "copier": None,
          "prv_mod": prov['prv_mod'],
          "prv_meth": prov['prv_meth'],
          "prv_ver": prov['prv_ver'],
          "prv_cmt": None,
          "md5": info[10].get('MD5'),
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

    def _log_error(self, event, index, err):
        mes = {
            'event': event,
            'index': index,
            'error': str(type(err))
        }
        with open('error.log', 'a') as f:
            f.write(json.dumps(mes))
            f.write('\n')

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
        return ret

    def _get_es_data_record(self, index, upa):
        eid = self._get_id(upa)
        res = self.es.get(index=index, routing=eid, doc_type='data', id=eid, ignore=404)
        if not res['found']:
            return None
        return res

    def _put_es_data_record(self, index, upa, doc, version=None, reindex=False):
        eid = self._get_id(upa)
        if reindex:
            res = self.es.index(index=index, parent=eid, doc_type='data',
                                id=eid, routing=eid, body=doc)
        elif version is None:
            res = self.es.create(index=index, parent=eid, doc_type='data',
                                 id=eid, routing=eid, body=doc, refresh=True)
        else:
            res = self.es.index(index=index, parent=eid, doc_type='data',
                                id=eid, routing=eid, version=version, body=doc,
                                refresh=True)
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

    def publish(self, wsid):
        # Find each index
        wsinfo = self._get_ws_info(wsid)
        public = wsinfo['public']

        if public:
            script = self.add_template.format(grp="-1")
        else:
            script = self.del_template.format(grp="-1")

        aq = {
                "query": {
                    "prefix": {"prefix": "WS:%d/" % (wsid)}
                    },
                "script": {
                    "source": script
                }
            }

        filt = {"bool": {
                    "filter": [
                        {"term": {"public": not public}},
                        {"term": {"accgrp": wsid}}
                    ]
                }}
        publics = "false"
        if public:
            publics = "true"
        dq = {
                "query": filt,
                "script": {
                    "source": "ctx._source.public=%s" % publics
                }
            }
        active_indexes = self._get_all_active_indexes()
        for index in active_indexes:
            res = self.es.update_by_query(index=index, doc_type='access',
                                          body=aq, ignore=[400, 404],
                                          refresh=True)
            res = self.es.update_by_query(index=index, doc_type='data',
                                          body=dq, ignore=[400, 404],
                                          refresh=True)

    def _get_all_active_indexes(self):
        indexes = []
        for oindex in self.mapping:
            for index in self.mapping[oindex]:
                indexes.append(index['index_name'])
        index_list = ','.join(indexes)
        active_indexes = self.es.indices.get(index_list, ignore_unavailable=True)

        return active_indexes

    def delete(self, event):
        # Find each index
        id = self._get_id(event['upa'])
        active_indexes = self._get_all_active_indexes()
        q = {
            'query': {
                'parent_id': {
                    'type': 'data',
                    'id': id
                }
            }
        }
        for index in active_indexes:
            res = self.es.delete_by_query(index=index, doc_type='data',
                                          routing=id, body=q, ignore=[400, 404],
                                          refresh=True)
            self.es.delete(index=index, doc_type='access', id=id, ignore=404,
                           refresh=True)

    def _update_es_access(self, index, wsid, objid, vers, upa):
        # Should pass a wsid but just in case...
        wsinfo = self._get_ws_info(wsid)
        if wsinfo['temp']:
            return None
        public = wsinfo['public']
        doc = self._access_rec(wsid, objid, vers, public=public)
        eid = self._get_id(upa)
        res = self.es.index(index=index, doc_type='access', id=eid, body=doc,
                            refresh=True)
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
            extra = self.mr.run(module, method, params)[0]
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

        eid = self._get_id(upa)
        res = self.es.get(index=index, doc_type='access', id=eid, ignore=404)
        if res['found']:
            self.log.info("%s already indexed in %s" % (eid, index))
            return

        doc = self._create_obj_rec(upa)
        params = {'upa': upa}
        (module, method) = oindex['index_method'].split('.')
        extra = self.mr.run(module, method, params)[0]
        self.mr.cleanup()
        if 'data' in extra and extra['data'] is not None:
            doc['keys'] = extra['data']
        doc['ojson'] = json.dumps(doc['keys'])
        self.log.debug(doc)
        self._update_es_access(index, wsid, objid, vers, upa)
        res = self._put_es_data_record(index, upa, doc)
        self.log.debug(res)
        oid = '%d/%s' % (wsid, objid)
        info = self.ws.get_object_info3({'objects': [{'ref': oid}]})['infos'][0]
        if info[4] == vers:
            self._update_islast(index, wsid, objid, info[4])

    def _new_object_version_feature_index(self, event, oindex):
        wsid = event['accgrp']
        objid = event['objid']
        vers = event['ver']
        upa = event['upa']
        index = oindex['index_name']
        self._check_mapping(oindex)

        # Check if any exists
        eid = self._get_id(upa)
        res = self.es.get(index=index, doc_type='access', id=eid, ignore=404)
        if res['found']:
            self.log.info("%s already indexed in %s" % (eid, index))
            return

        doc = self._create_obj_rec(upa)
        params = {'upa': upa}
        (module, method) = oindex['index_method'].split('.')
        extra = self.mr.run(module, method, params)[0]
        self.mr.cleanup()
        parent = extra['parent']
        if 'features' in extra and extra['features'] is not None:
            features = extra['features']
        recs = []
        doc['pjson'] = json.dumps(parent)
        pguid = self._get_id(upa)
        bdoc = []
        ct = 0
        for row in extra['features']:
            doc['keys'] = {**parent, **row}
            guid = row.pop('guid')
            if not guid.startswith('WS:'):
                guid = "WS:" + guid
            guid = guid.replace('/', ':')
            doc['guid'] = guid
            doc['ojson'] = json.dumps(doc['keys'])
            rec = {'_id': guid, '_source': doc, '_index': index,
                   '_parent': pguid, '_type': 'data'}
            bdoc.append(rec)
            ct += 1
            if ct > BULK_MAX:
                bulk(self.es, bdoc)
                bdoc = []
                ct = 0

        if ct > 0:
            bulk(self.es, bdoc)

        self._update_es_access(index, wsid, objid, vers, upa)
        oid = '%d/%s' % (wsid, objid)
        info = self.ws.get_object_info3({'objects': [{'ref': oid}]})['infos'][0]
        if info[4] == vers:
            self._update_islast(index, wsid, objid, info[4])

    def _update_islast(self, index, wsid, objid, vers):
        prefix = "WS:%d/%s" % (wsid, objid)
        doc = {"query": {"bool": {"filter": [{"term": {"prefix": prefix}}]}},
               "script": {"source": "ctx._source.islast = (ctx._source.version == params.lastver)",
                          "params": {"lastver": int(vers)}}}
        res = self.es.update_by_query(index, 'data', doc, refresh=True)

    def new_object_version(self, event):
        # For a NEW ALL VERSION we will just index the latest versions
        #
        if event['evtype'] == 'NEW_ALL_VERSIONS':
            upa = "%s/%s" % (event['accgrp'], event['objid'])
            info = self.ws.get_object_info3({'objects': [{'ref': upa}]})['infos'][0]
            vers = info[4]
            event['ver'] = vers
            (event['objtype'], event['objtypever']) = info[2].split('-')
            event['upa'] = '%s/%s' % (upa, vers)

        indexes = self._get_indexes(event['objtype'])
        for oindex in indexes:
            try:
                if 'feature' in oindex and oindex['feature']:
                    self._new_object_version_feature_index(event, oindex)
                else:
                    self._new_object_version_index(event, oindex)
            except Exception as e:
                self.log.error("Failed for index")
                self._log_error(event, oindex, e)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.log.info("*** print_tb:")
                traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
                self.log.info("*** print_exception:")
                traceback.print_exception(exc_type, exc_value, exc_traceback,
                                          limit=2, file=sys.stdout)
        self.log.info("Completed new object version")
        return True
