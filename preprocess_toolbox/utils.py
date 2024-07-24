import importlib
import logging


def get_implementation(location):
    module_ref, object_name = location.split(":")
    implementation = None

    try:
        module = importlib.import_module(module_ref)
        implementation = getattr(module, object_name)
    except ImportError:
        logging.exception("Unable to import from location: {}".format(location))

    return implementation
