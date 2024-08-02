import logging
import os

import orjson


def get_config(loader_config: os.PathLike):
    with open(loader_config, "r") as fh:
        logging.info("Configuration {} being loaded".format(fh.name))
        cfg_data = orjson.loads(fh.read())
    return cfg_data


def update_config(loader_config: os.PathLike,
                  segment: str,
                  configuration: dict):
    cfg_data = get_config(loader_config)

    if segment not in cfg_data:
        cfg_data[segment] = dict()
    cfg_data[segment].update(configuration)

    with open(loader_config, "w") as fh:
        logging.info("Writing over {}".format(fh.name))
        fh.write(orjson.dumps(cfg_data, option=orjson.OPT_INDENT_2).decode())

