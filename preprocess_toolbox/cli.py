import argparse
import collections
import datetime as dt
import logging
import re

import pandas as pd

from download_toolbox.interface import Frequency, get_dataset_config_implementation


def date_arg(string: str) -> object:
    """

    :param string:
    :return:
    """
    date_match = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", string)
    return dt.date(*[int(s) for s in date_match.groups()])


def dates_arg(string: str) -> object:
    """

    :param string:
    :return:
    """
    if string == "none":
        return []

    date_match = re.findall(r"(\d{4})-(\d{1,2})-(\d{1,2})", string)

    if len(date_match) < 1:
        raise argparse.ArgumentError(argument="dates",
                                     message="No dates found for supplied argument {}".format(string))
    return [dt.date(*[int(s) for s in date_tuple]) for date_tuple in date_match]


def csv_arg(string: str) -> list:
    """

    :param string:
    :return:
    """
    csv_items = []
    string = re.sub(r'^\'(.*)\'$', r'\1', string)

    for el in string.split(","):
        if len(el) == 0:
            csv_items.append(None)
        else:
            csv_items.append(el)
    return csv_items


def csv_of_csv_arg(string: str) -> list:
    """

    :param string:
    :return:
    """
    csv_items = []
    string = re.sub(r'^\'(.*)\'$', r'\1', string)

    for el in string.split(","):
        if len(el) == 0:
            csv_items.append(None)
        else:
            csv_items.append(el.split("|"))
    return csv_items


def csv_of_date_args(string: str) -> list:
    """

    :param string:
    :return:
    """
    csv_items = []
    string = re.sub(r'^\'(.*)\'$', r'\1', string)

    for el in string.split(","):
        if len(el) == 0:
            csv_items.append(None)
        else:
            csv_items.append([date_arg(date) for date in el.split("|")])
    return csv_items


def int_or_list_arg(string: str) -> object:
    """

    :param string:
    :return:
    """
    try:
        val = int(string)
    except ValueError:
        val = string.split(",")
    return val

class BaseArgParser(argparse.ArgumentParser):
    """An ArgumentParser specialised to support common argument handling

    The 'allow_*' methods return self to permit method chaining.

    :param suppress_logs:
    """

    def __init__(self,
                 *args,
                 suppress_logs=None,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self._suppress_logs = suppress_logs

        self.add_argument("-v",
                          "--verbose",
                          action="store_true",
                          default=False)

    def add_extra_args(self, extra_args):
        for arg in extra_args:
            self.add_argument(*arg[0], **arg[1])
        return self

    def parse_args(self, *args, **kwargs):
        args = super().parse_args(*args, **kwargs)

        loglevel = logging.DEBUG if args.verbose else logging.INFO
        logging.basicConfig(level=loglevel)
        logging.getLogger().setLevel(loglevel)

        if self._suppress_logs is not None and type(self._suppress_logs) is list:
            for log_module in self._suppress_logs:
                logging.debug("Setting {} to WARNING only".format(log_module))
                logging.getLogger(log_module).setLevel(logging.WARNING)
        logging.getLogger("matplotlib").setLevel(logging.WARNING)

        return args


class ProcessingArgParser(BaseArgParser):
    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.add_argument("source", type=str)

    def add_ref_ds(self):
        self.add_argument("reference", type=str)
        return self

    def add_destination(self, optional: bool = True):
        if optional:
            self.add_argument("destination_id", type=str, nargs="?", default=None)
        else:
            self.add_argument("destination_id", type=str)
        return self

    def add_loader(self):
        self.add_argument("-u",
                          "--update-key",
                          default=None,
                          help="Add update key to processor to avoid overwriting default"
                               "entries in the loader configuration",
                          type=str)
        return self

    def add_concurrency(self):
        self.add_argument("-po",
                          "--parallel-opens",
                          default=False,
                          action="store_true",
                          help="Allow parallel opens and dask implementation")
        return self

    def add_implementation(self):
        self.add_argument("-i",
                          "--implementation",
                          type=str,
                          help="Allow implementation to be specified on command line")
        return self

    def add_splits(self):
        self.add_argument("-ps",
                          "--processing-splits",
                          type=csv_arg,
                          required=False,
                          default=None)
        self.add_argument("-sn",
                          "--split-names",
                          type=csv_arg,
                          required=False,
                          default=None)
        self.add_argument("-ss",
                          "--split_starts",
                          type=csv_of_date_args,
                          required=False,
                          default=[])
        self.add_argument("-se",
                          "--split_ends",
                          type=csv_of_date_args,
                          required=False,
                          default=[])
        return self

    def add_vars(self):
        self.add_argument("--abs",
                          help="Comma separated list of absolute vars",
                          type=csv_arg,
                          default=[])
        self.add_argument("--anom",
                          help="Comma separated list of anomoly vars",
                          type=csv_arg,
                          default=[])
        return self

    def add_var_name(self):
        self.add_argument("-n", "--var-names",
                          help="Comma separated list of variable names",
                          type=csv_arg,
                          default=None)
        return self

    def add_trends(self):
        self.add_argument("--trends",
                          help="Comma separated list of abs vars",
                          type=csv_arg,
                          default=[])
        self.add_argument("--trend-lead",
                          help="Time steps in the future for linear trends",
                          type=int_or_list_arg,
                          default=7)
        return self

    def add_reference(self):
        self.add_argument("-r",
                          "--ref",
                          help="Reference loader for normalisations etc",
                          default=None,
                          type=str)
        return self


def process_split_args(args: object,
                       frequency: Frequency) -> dict:
    """

    :param args:
    :param frequency:
    :return:
    """
    splits = {_: list() for _ in args.split_names}

    for idx, split in enumerate(splits.keys()):
        split_dates = collections.deque()

        for period_start, period_end in zip(args.split_starts[idx], args.split_ends[idx]):
            split_dates += [
                pd.to_datetime(date).date()
                for date in pd.date_range(period_start, period_end, freq=frequency.freq)
            ]
        logging.info("Got {} dates for {}".format(len(split_dates), split))

        splits[split] = sorted(list(split_dates))
    return splits
