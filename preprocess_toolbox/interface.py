import logging
import os
import sys

import numpy as np
import orjson

from preprocess_toolbox.base import PreProcessor
from download_toolbox.interface import Location, Frequency


def get_preproc_config_implementation(config: os.PathLike):
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

    dtype = getattr(np, **cfg["_dtype"])
    freq_dict = {k.strip("_"): getattr(Frequency, v) for k, v in cfg.items() if v in list(Frequency.__members__)}
    remaining = {k.strip("_"): v
                 for k, v in cfg.items()
                 if k not in [*["_{}".format(el) for el in freq_dict.keys()], "_dtype"]}

    create_kwargs = dict(dtype=dtype, **remaining, **freq_dict)
    logging.info("Attempting to instantiate {} with loaded configuration".format(implementation))
    logging.debug("Converted kwargs from the retrieved configuration: {}".format(create_kwargs))

    return PreProcessor(**create_kwargs)
