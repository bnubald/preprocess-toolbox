import logging
import os

import orjson

from download_toolbox.interface import get_dataset_config_implementation


def get_config(loader_config: os.PathLike):
    with open(loader_config, "r") as fh:
        logging.info("Configuration {} being loaded".format(fh.name))
        cfg_data = orjson.loads(fh.read())
    return cfg_data


def get_processed_path_for_dataset(loader_config: os.PathLike,
                                   dataset_config: os.PathLike):

    cfg_data = [cfg
                for cfg in get_config(loader_config)["sources"].values()
                if cfg["dataset_config"] ==
                get_dataset_config_implementation(dataset_config).config_file]

    if len(cfg_data) != 1:
        raise RuntimeError("Cannot find single processed dataset in {} corresponding to {}".
                           format(loader_config, dataset_config))
    return cfg_data[0]["path"]


def update_config(loader_config: os.PathLike,
                  segment: str,
                  configuration: dict):
    print(loader_config, segment, configuration)

    cfg_data = get_config(loader_config)

    if segment not in cfg_data:
        cfg_data[segment] = dict()
    cfg_data[segment].update(configuration)

    with open(loader_config, "w") as fh:
        logging.info("Writing over {}".format(fh.name))
        fh.write(orjson.dumps(cfg_data).decode())

