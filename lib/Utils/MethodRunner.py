from Catalog.CatalogClient import Catalog
import docker
from configparser import ConfigParser
import os
import uuid
import json


class ServerError(Exception):

    def __init__(self, name, code, message, data=None, error=None):
        super(Exception, self).__init__(message)
        self.name = name
        self.code = code
        self.message = '' if message is None else message
        self.data = data or error or ''
        # data = JSON RPC 2.0, error = 1.1

    def __str__(self):
        return self.name + ': ' + str(self.code) + '. ' + self.message + \
            '\n' + self.data


class MethodRunner:

    def __init__(self, config, token=None):
        self.catalog = Catalog(config.get('catalog-service-url'))
        self.docker = docker.from_env()
        self.config = config
        self.scratch = config.get('scratch', '/kb/module/work/tmp')
        self.token = token

    def _create_config_properties(self):
        config = ConfigParser()
        config['global'] = {
          'kbase_endpoint': self.config.get('kbase-endpoint'),
          'workspace_url': self.config.get('workspace-url'),
          'shock_url': self.config.get('shock-url'),
          'handle_url': self.config.get('kbase-endpoint'),
          'auth_service_url': self.config.get('auth-service-url'),
          'auth_service_url_allow_insecure': self.config.get('auth-service-url-allow-insecure'),
          'ws_admin_token': self.config.get('ws-admin-token'),
          'scratch': self.config.get('scratch')
           }
        return config

    def run(self, module, method, params, version=None):
        """
        Look up and run the module/method with the specified
        parameters.
        """

        # Look up the module info
        req = {'module_name': module}
        if version is not None:
            req['version'] = version
        res = self.catalog.get_module_version(req)
        image = res['docker_img_name']
        list = self.docker.images.list()

        # Pull the image if we don't have it
        pulled = False
        for im in list:
            if image in im.tags:
                print("Already pulled")
                id = im.id
                pulled = True
        if not pulled:
            print("Pulling %s" % (image))
            id = self.docker.images.pull(image).id

        # Prepare the run space
        job_id = str(uuid.uuid1())
        print("image id=%s job_id=%s" % (id, job_id))

        job_dir = self.scratch + '/' + job_id
        os.makedirs(job_dir)
        # Create config.properties
        config = self._create_config_properties()

        with open(job_dir + '/config.properties', 'w') as configfile:
            config.write(configfile)
        # Create input.json
        input = {
            "version": "1.1",
            "method": module + '.' + method,
            "params": [params],
            "context": dict()
            }
        ijson = job_dir + '/input.json'
        with open(ijson, 'w') as f:
            f.write(json.dumps(input))

        with open(job_dir + '/token', 'w') as f:
            f.write(self.token)

        # scratch = /kb/module/work/tmp

        #
        #
        #
        # [global]

        # Run the container
        vols = {
                job_dir: {'bind': '/kb/module/work', 'mode': 'rw'}
                }
        env = {
                'SDK_CALLBACK_URL': 'not_supported_yet'
        }
        try:
            self.docker.containers.run(image, 'async',
                                       environment=env,
                                       volumes=vols)
        except docker.errors.ContainerError as e:
            print(e.stderr)
        # print(stderr)
        # print(stdout)
        output = None
        out_file = job_dir + '/output.json'
        if os.path.exists(out_file):
            with open(out_file) as f:
                data = f.read()
            output = json.loads(data)
        else:
            raise OSError('No output json')

        if 'error' in output:
            raise ServerError(**output['error'])

        return output
