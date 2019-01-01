from Workspace.WorkspaceClient import Workspace
import json
wsid = 15792
upa = '15792/2'

ws = Workspace('https://ci.kbase.us/services/ws')
#d = ws.get_workspace_info({'id': wsid})
#with open('get_workspace_info.json', 'w') as f:
#    f.write(json.dumps(d))
#d = ws.list_objects({'ids': [wsid]})
#with open('list_objects.json', 'w') as f:
#    f.write(json.dumps(d))
d = ws.get_objects2({'objects': [{'ref': upa}], 'no_data': 1})
with open('genome_object.json', 'w') as f:
    f.write(json.dumps(d))
d = ws.get_object_info3({'objects': [{'ref': upa}], 'includeMetadata':1})
with open('genome_object_info.json', 'w') as f:
    f.write(json.dumps(d))
