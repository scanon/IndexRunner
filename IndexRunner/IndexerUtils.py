import json
import logging
import os
import re
import sys
import traceback
from time import time

import yaml
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from IndexRunner.EventProducer import EventProducer
from IndexRunner.MethodRunner import MethodRunner
from IndexRunner.WSAdminUtils import WorkspaceAdminUtil

# This is the interface that will process indexing
BULK_MAX = 9
_MAX_LIST = 10000

_ADD_TEMPLATE = """
if (ctx._source.lastin.indexOf({grp}) < 0) {{
    ctx._source.lastin.add({grp});
}}
if (ctx._source.groups.indexOf({grp}) < 0) {{
  ctx._source.groups.add({grp});
}}
"""

_DEL_TEMPLATE = """
if (ctx._source.lastin.indexOf({grp}) >= 0) {{
  ctx._source.lastin.remove(ctx._source.lastin.indexOf({grp}));
}}
if (ctx._source.groups.indexOf({grp}) >= 0) {{
  ctx._source.groups.remove(ctx._source.groups.indexOf({grp}));
}}
"""


class IndexerUtils:

    def __init__(self, config):
        self.log = logging.getLogger('indexrunner')
        self.ws = WorkspaceAdminUtil(config)
        self.es = Elasticsearch([config['elastic-host']])
        self.esbase = config['elastic-base']
        mapfile = config.get('mapping-file')
        self.log.info("Mapping File: %s" % (mapfile))
        self.mapping = self._read_mapfile(mapfile)

        if config.get('workspace-admin-token'):
            token = config['workspace-admin-token']
        else:
            token = os.environ.get('KB_AUTH_TOKEN')
        self.mr = MethodRunner(config, token=token)
        self.ep = EventProducer(config)
        # TODO: access and data specs are not used?
        with open('specs/mapping.yml') as f:
            self.mapping_spec = yaml.load(f)

    def _read_mapfile(self, mapfile):
        with open(mapfile) as f:
            d = f.read()
        mapping = yaml.load(d)['types']
        for typ in mapping.keys():
            for index in mapping[typ]:
                name = index['index_name']
                index['index_name'] = '%s.%s' % (self.esbase, name)
        return mapping

    def process_event(self, evt):
        etype = evt['evtype']
        ws = evt['accgrp']
        if evt.get('ver'):
            evt['upa'] = '%d/%s/%d' % (evt['accgrp'], evt['objid'], evt['ver'])
        if etype in ['NEW_VERSION', 'NEW_ALL_VERSIONS']:
            self.new_object_version(evt)
        elif 'PUBLISH' in etype:
            self.publish(evt['accgrp'])
        elif etype.startswith('DELETE_'):
            self.delete(evt)
        elif etype == 'COPY_ACCESS_GROUP':
            self._index_workspace(ws)
        elif etype == 'RENAME_ALL_VERSIONS':
            self.log.warning("Warning rename not implemented.")
        elif etype in ['REINDEX_WORKSPACE']:
            # Pseudo event
            self._index_workspace(ws)
        else:
            self.log.error("Can't process evtype " + evt['evtype'])

    def _index_workspace(self, wsid):
        """
        List the workspace and generate an index event for each object.
        """
        minimum = 0
        while True:
            objs = self.ws.list_objects({'ids': [wsid], 'minObjectID': minimum, 'limit': _MAX_LIST})
            self.ep.index_objects(objs)
            if len(objs) <= _MAX_LIST:
                break
            minimum = objs[-1][0] + 1

    def _create_obj_rec(self, upa):
        (wsid, objid, vers) = self._split_upa(upa)
        wsinfo = self._get_ws_info(wsid)
        # Don't index temporary narratives
        if wsinfo['temp']:
            return None
        req = {'objects': [{'ref': upa}], 'no_data': 1}
        obj = self.ws.get_objects2(req)['data'][0]
        info = obj['info']
        prov = self._get_prov(obj)
        return {
          "guid": f"WS:{upa}",
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
          "md5": info[8],
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

    # TODO: should we just add an filehandler for this?
    def _log_error(self, event, index, err):
        mes = {
            'event': event,
            'index': index,
            'error': str(type(err))
        }
        with open('error.log', 'a') as f:
            f.write(json.dumps(mes))
            f.write('\n')

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
            "pguid": f"WS:{wsid}/{objid}/{vers}",
            "prefix": f"WS:{wsid}/{objid}",
            "version": vers
        }
        if public:
            rec['lastin'].append(-1)
            rec['groups'].append(-1)
        # type": "access"
        return rec

    def _get_es_id(self, upa):
        """
        Return the UPA of an object as an elastic id (with colon delimiters)
        """
        if not re.match(r'^\d+\/\d+\/\d+$', upa):
            raise ValueError(f"'{upa}' is not an upa")
        return f"WS:{upa.replace('/', ':')}"

    def _get_prov(self, obj):
        """
        Fetch provenance data from an object returned by get_objects2.
        Returns a dictionary of module, method, version, and comment.
        """
        if not obj.get('provenance'):  # must be non-empty list
            return None
        prov = obj['provenance'][0]
        ret = {
          "prv_mod": prov.get('service'),
          "prv_meth": prov.get('method'),
          "prv_ver": prov.get('service_ver'),
          "prv_cmt": prov.get('description')
        }
        if 'script' in prov:
            ret['prv_mod'] = 'legacy_transform'
            ret['prv_meth'] = prov['script']
            ret['prv_ver'] = prov.get('script_ver')
        return ret

    def _put_es_data_record(self, index, upa, doc):
        """
        Add an ES data record.
        Only works if the object hasn't been indexed before.  Will throw an
        error if it has been indexed already.
        """
        eid = self._get_es_id(upa)
        res = self.es.create(index=index, parent=eid, doc_type='data',
                             id=eid, routing=eid, body=doc, refresh=True)
        return res

    def _get_ws_info(self, wsid):
        """
        Fetch workspace the info tuple
        https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.workspace_info
        """
        info = self.ws.get_workspace_info({'id': wsid})
        meta = info[8]
        # Don't index temporary narratives
        temp = meta.get('is_temporary') == 'true'
        public = (info[6] != 'n')
        # TODO
        shared = False
        return {'wsid': wsid, 'info': info, 'meta': meta,
                'temp': temp, 'public': public, 'shared': shared}

    def publish(self, wsid):
        """This updates the visibility of objects when a workspace is made public"""
        # Find each index
        wsinfo = self._get_ws_info(wsid)
        public = wsinfo['public']

        if public:
            script = _ADD_TEMPLATE.format(grp="-1")
        else:
            script = _DEL_TEMPLATE.format(grp="-1")

        aq = {
            "query": {
                "prefix": {"prefix": f"WS:{wsid:d}/"}
            },
            "script": {
                "source": script
            }
        }

        filt = {
            "bool": {
                "filter": [
                    {"term": {"public": not public}},
                    {"term": {"accgrp": wsid}}
                ]
            }
        }
        publics = "true" if public else "false"
        dq = {
            "query": filt,
            "script": {
                "source": f"ctx._source.public={publics}"
            }
        }
        active_indexes = self._get_all_active_indexes()
        for index in active_indexes:
            self.es.update_by_query(index=index, doc_type='access',
                                    body=aq, ignore=[400, 404],
                                    refresh=True)
            self.es.update_by_query(index=index, doc_type='data',
                                    body=dq, ignore=[400, 404],
                                    refresh=True)

    def _get_all_active_indexes(self):
        indexes = (index['index_name']
                   for oindex in self.mapping
                   for index in self.mapping[oindex])
        return self.es.indices.get(','.join(indexes), ignore_unavailable=True)

    def delete(self, event):
        # Find each index
        es_id = self._get_es_id(event['upa'])
        active_indexes = self._get_all_active_indexes()
        q = {
            'query': {
                'parent_id': {
                    'type': 'data',
                    'id': es_id
                }
            }
        }
        for index in active_indexes:
            self.es.delete_by_query(index=index, doc_type='data', routing=es_id,
                                    body=q, ignore=[400, 404], refresh=True)
            self.es.delete(index=index, doc_type='access', id=id, ignore=404,
                           refresh=True)

    def _update_es_access(self, index, wsid, objid, vers, upa):
        # Should pass a wsid but just in case...
        wsinfo = self._get_ws_info(wsid)
        if wsinfo['temp']:
            return None
        public = wsinfo['public']
        doc = self._access_rec(wsid, objid, vers, public=public)
        eid = self._get_es_id(upa)
        res = self.es.index(index=index, doc_type='access', id=eid, body=doc, refresh=True)
        return res

    def _split_upa(self, upa):
        return [int(x) for x in upa.split('/')]

    def _get_indexes(self, otype):
        # TODO: handle generics here
        if otype in self.mapping:
            return self.mapping[otype]
        return self.mapping['Other']

    def _ensure_mapping_exists(self, oindex, objschema):
        """Ensures a mapping exists in ES for 'index_name'"""
        index = oindex['index_name']
        res = self.es.indices.exists(index=index)
        if not res:
            schema = self.mapping_spec
            if oindex.get('raw'):
                schema = {'mappings': {'data': {'properties': objschema}}}
            elif objschema is not None:
                schema['mappings']['data']['properties']['key'] = \
                    {'properties': objschema}
            self.es.indices.create(index=index, body=schema)

    def _new_raw_version_index(self, event, oindex):
        """This handles indexing an object where the callout is expected to
        return an entire ElasticSearch reccord for storage"""
        upa = event['upa']
        index = oindex['index_name']
        eid = self._get_es_id(upa)
        res = self.es.get(index=index, doc_type='data', id=eid, ignore=404)
        if res.get('status') != 404 and res['found']:
            self.log.info("%s already indexed in %s" % (eid, index))
            return

        params = {'upa': upa}
        (module, method) = oindex['index_method'].split('.')
        resp = self.mr.run(module, method, params)[0]
        self.mr.cleanup()
        if resp.get('data') is None:
            raise ValueError(f"{oindex['index_method']} did not return 'data' for {event}")
        self._ensure_mapping_exists(oindex, resp['schema'])
        doc = resp['data']
        self.es.create(index=index, doc_type='data', id=eid, body=doc, refresh=True)

    def _new_object_version_index(self, event, oindex):
        """
        This handles indexing a specific object version.
        The callout should return a structure with a 'data'
        """
        wsid = event['accgrp']
        objid = event['objid']
        vers = event['ver']
        upa = event['upa']
        index = oindex['index_name']

        eid = self._get_es_id(upa)
        res = self.es.get(index=index, doc_type='access', id=eid, ignore=404)
        if res.get('status') != 404 and res['found']:
            self.log.info("%s already indexed in %s" % (eid, index))
            return

        doc = self._create_obj_rec(upa)
        params = {'upa': upa}
        extra = {}  # type: dict
        schema = None
        if 'default_indexer' not in oindex['index_method']:
            (module, method) = oindex['index_method'].split('.')
            extra = self.mr.run(module, method, params)[0]
            self.mr.cleanup()
            schema = extra['schema']
        self._ensure_mapping_exists(oindex, schema)
        if extra.get('data') is not None:
            doc['keys'] = extra['data']
            doc['ojson'] = json.dumps(doc['keys'])
        else:
            self.log.warning(f"{oindex['index_method']} did not return 'data' for {event}")
        self._update_es_access(index, wsid, objid, vers, upa)
        self._put_es_data_record(index, upa, doc)
        oid = f'{wsid:d}/{objid}'
        info = self.ws.get_object_info3({'objects': [{'ref': oid}]})['infos'][0]
        if info[4] == vers:
            self._update_islast(index, wsid, objid, vers)

    def _new_object_version_multi_index(self, event, oindex):
        """
        This handles indexing multiple sub-objects for a specific object version.
        The callout should return a structure with a 'features' that
        is a list of dictionary keys
        """
        wsid = event['accgrp']
        objid = event['objid']
        vers = event['ver']
        upa = event['upa']
        index = oindex['index_name']

        # Check if any exists
        eid = self._get_es_id(upa)
        res = self.es.get(index=index, doc_type='access', id=eid, ignore=404)
        if res.get('status') != 404 and res['found']:
            self.log.info(f"{eid} already indexed in {index}")
            return

        doc = self._create_obj_rec(upa)
        params = {'upa': upa}
        (module, method) = oindex['index_method'].split('.')
        extra = self.mr.run(module, method, params)[0]
        self.mr.cleanup()
        parent = extra['parent']
        self._ensure_mapping_exists(oindex, extra['schema'])
        doc['pjson'] = json.dumps(parent)
        pguid = self._get_es_id(upa)
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
        oid = f'{wsid:d}/{objid}'
        info = self.ws.get_object_info3({'objects': [{'ref': oid}]})['infos'][0]
        if info[4] == vers:
            self._update_islast(index, wsid, objid, info[4])

    def _update_islast(self, index, wsid, objid, vers):
        prefix = "WS:%d/%s" % (wsid, objid)
        doc = {"query": {"bool": {"filter": [{"term": {"prefix": prefix}}]}},
               "script": {"source": "ctx._source.islast = (ctx._source.version == params.lastver)",
                          "params": {"lastver": int(vers)}}}
        self.es.update_by_query(index, 'data', doc, refresh=True)

    def new_object_version(self, event):
        # For a NEW ALL VERSION we will just index the latest versions
        #
        if event['evtype'] == 'NEW_ALL_VERSIONS':
            upa = f"{event['accgrp']}/{event['objid']}"
            info = self.ws.get_object_info3({'objects': [{'ref': upa}]})['infos'][0]
            vers = info[4]
            event['ver'] = vers
            (event['objtype'], event['objtypever']) = info[2].split('-')
            event['upa'] = f'{upa}/{vers}'

        indexes = self._get_indexes(event['objtype'])
        for oindex in indexes:
            try:
                if oindex.get('multi'):
                    self._new_object_version_multi_index(event, oindex)
                elif oindex.get('raw'):
                    self._new_raw_version_index(event, oindex)
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
