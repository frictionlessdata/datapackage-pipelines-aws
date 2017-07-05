import os
import logging

def generate_path(file_path, base_path='', datapackage={}):
    vers = datapackage.get('version', 'latest')
    owner = ''
    pkg_name = datapackage.get('name')
    if '{owner}' in base_path:
        try:
            owner = datapackage['owner']
        except KeyError:
            logging.error('datapackage.json should have "owner" property')
            raise
    base_path = base_path.format(owner=owner, name=pkg_name, version=vers)
    return os.path.join(base_path, file_path)
