import importlib
import logging
import operator
import os

from dateutil.relativedelta import relativedelta

import orjson

from download_toolbox.interface import get_implementation, DatasetConfig


def get_config(loader_config: os.PathLike):
    with open(loader_config, "r") as fh:
        logging.info("Configuration {} being loaded".format(fh.name))
        cfg_data = orjson.loads(fh.read())
    return cfg_data


def get_extension_dates(ds_config: DatasetConfig,
                        dates: list,
                        num_steps: int,
                        reverse=False):
    additional_dates, dropped_dates = [], []

    for date in dates:
        for time in range(num_steps):
            attrs = {"{}s".format(ds_config.frequency.attribute): time + 1}
            op = operator.sub if reverse else operator.add
            extended_date = op(date, relativedelta(**attrs))

            if extended_date not in dates:
                if all([os.path.exists(ds_config.var_filepath(var_config, [extended_date]))
                        for var_config in ds_config.variables]):
                    # We only add these dates into the mix if all necessary files exist
                    additional_dates.append(extended_date)
                else:
                    # Otherwise, warn that the lag data means this is being dropped
                    logging.warning("{} will be dropped due to missing data {}".
                                    format(date, extended_date))
                    dropped_dates.append(date)

    return sorted(list(set(additional_dates))), sorted(list(set(dropped_dates)))
