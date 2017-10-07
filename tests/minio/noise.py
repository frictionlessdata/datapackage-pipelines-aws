from datapackage_pipelines.wrapper import spew, ingest
from datapackage_pipelines.utilities.resources import PROP_STREAMING


def get_resource(i):
    for j in range(10000):
        yield {"i": i, "j": j}


def get_resources():
    for i in range(5):
        yield get_resource(i)


def get_resource_descriptors():
    for i in range(5):
        yield {"name": "noise{}".format(i),
               "schema": {"fields": [{"name": "i", "type": "number"},
                                     {"name": "j", "type": "number"}]},
               "path": "noise{}.csv".format(i),
               PROP_STREAMING: True,}


def get_datapackage():
    return {"name": "noise", "resources": list(get_resource_descriptors())}


parameters, datapackage, resources = ingest()
spew(get_datapackage(), get_resources())
