import logging
import os
import sys

import orjson


class ProcessorFactory(object):
    @classmethod
    def get_item(cls, impl):
        klass_name = ProcessorFactory.get_klass_name(impl)

        # This looks weird, but to avoid circular imports it helps to isolate implementations
        # herein, so that dependent libraries can more easily import functionality without
        # accidentally importing everything through download_toolbox.data
        if hasattr(sys.modules[__name__], klass_name):
            return getattr(sys.modules[__name__], klass_name)

        logging.error("No class named {0} found in preprocessor_toolbox.data".format(klass_name))
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
    logging.debug("Loaded configuration {}".format(cfg))
    cfg, implementation = cfg["data"], cfg["implementation"]

    remaining = {k.strip("_"): v for k, v in cfg.items()}

    create_kwargs = dict(**remaining)
    logging.info("Attempting to instantiate {} with loaded configuration".format(implementation))
    logging.debug("Converted kwargs from the retrieved configuration: {}".format(create_kwargs))

    return ProcessorFactory.get_item(implementation)(**create_kwargs)
