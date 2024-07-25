import logging
import os
import sys

import orjson

from download_toolbox.interface import get_dataset_config_implementation
from preprocess_toolbox.utils import get_implementation


class ProcessorFactory(object):
    @classmethod
    def get_item(cls, impl):
        klass_name = ProcessorFactory.get_klass_name(impl)

        # This looks weird, but to avoid circular imports it helps to isolate implementations
        # herein, so that dependent libraries can more easily import functionality without
        # accidentally importing everything through download_toolbox.data
        if hasattr(sys.modules[__name__], klass_name):
            return getattr(sys.modules[__name__], klass_name)

        logging.error("No class named {0} found in ".format(klass_name))
        raise ReferenceError

    @classmethod
    def get_klass_name(cls, name):
        return name.split(":")[-1]


def get_processor_implementation(config: os.PathLike):
    if not str(config).endswith(".json"):
        raise RuntimeError("{} does not look like a JSON configuration".format(config))
    if not os.path.exists(config):
        raise RuntimeError("{} is not a configuration in existence".format(config))

    logging.debug("Retrieving implementations details from {}".format(config))

    with open(config) as fh:
        data = fh.read()

    cfg = orjson.loads(data)
    cfg, implementation = cfg["data"], cfg["implementation"]

    remaining = {k.strip("_"): v for k, v in cfg.items()}

    create_kwargs = dict(**remaining)
    logging.info("Attempting to instantiate {} with loaded configuration".format(implementation))
    logging.debug("Converted kwargs from the retrieved configuration: {}".format(create_kwargs))

    return ProcessorFactory.get_item(implementation)(**create_kwargs)


def get_processor_from_source(identifier: str, source_cfg: dict):
    if "dataset_config" not in source_cfg:
        raise RuntimeError("Source configuration should link to a dataset!")
    if "implementation" not in source_cfg:
        raise RuntimeError("Must specify the implementation to use!")

    create_kwargs = {k: v for k, v in source_cfg.items() if k not in ["dataset_config", "implementation",]}
    logging.info("Attempting to instantiate {} with loaded configuration".format(source_cfg["implementation"]))
    logging.debug("Converted kwargs from the retrieved configuration: {}".format(create_kwargs))

    return get_implementation(source_cfg["implementation"])(
        get_dataset_config_implementation(source_cfg["dataset_config"]),
        identifier=identifier,
        init_source=False,
        **create_kwargs)

