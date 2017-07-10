import os
import logging


def generate_path(file_path, base_path='', datapackage={}):
    format_params = {'version': 'latest'}
    format_params.update(datapackage)
    try:
        base_path = base_path.format(**format_params)
    except KeyError:
        logging.error('datapackage.json is missing property: %s' % KeyError)
        raise
    return os.path.join(base_path, file_path)
