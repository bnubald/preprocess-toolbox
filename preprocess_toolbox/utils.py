import importlib
import logging
import os

import orjson

from download_toolbox.interface import get_implementation


def get_config(loader_config: os.PathLike):
    with open(loader_config, "r") as fh:
        logging.info("Configuration {} being loaded".format(fh.name))
        cfg_data = orjson.loads(fh.read())
    return cfg_data
