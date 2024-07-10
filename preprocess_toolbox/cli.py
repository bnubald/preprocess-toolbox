import argparse
import collections
import datetime as dt
import logging
import re

import pandas as pd


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


class ProcessingArgParser(argparse.ArgumentParser):
    """An ArgumentParser specialised to support forecast plot arguments

    The 'allow_*' methods return self to permit method chaining.

    :param suppress_logs:
    """

    def __init__(self,
                 *args,
                 suppress_logs=None,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self._suppress_logs = suppress_logs

        self.add_argument("source", type=str)

        self.add_argument("-v",
                          "--verbose",
                          action="store_true",
                          default=False)

    def add_destination(self):
        self.add_argument("destination", type=str, nargs="?", default=None)
        return self

    def add_loader_args(self):
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

    def add_split_date_args(self):
        self.add_argument("-ns",
                          "--train_start",
                          type=dates_arg,
                          required=False,
                          default=[])
        self.add_argument("-ne",
                          "--train_end",
                          type=dates_arg,
                          required=False,
                          default=[])
        self.add_argument("-vs",
                          "--val_start",
                          type=dates_arg,
                          required=False,
                          default=[])
        self.add_argument("-ve",
                          "--val_end",
                          type=dates_arg,
                          required=False,
                          default=[])
        self.add_argument("-ts",
                          "--test-start",
                          type=dates_arg,
                          required=False,
                          default=[])
        self.add_argument("-te",
                          "--test-end",
                          dest="test_end",
                          type=dates_arg,
                          required=False,
                          default=[])
        return self

    def add_var_args(self):
        self.add_argument("--abs",
                          help="Comma separated list of abs vars",
                          type=csv_arg,
                          default=[])
        self.add_argument("--anom",
                          help="Comma separated list of abs vars",
                          type=csv_arg,
                          default=[])
        return self

    def add_trend_args(self):
        self.add_argument("--trends",
                          help="Comma separated list of abs vars",
                          type=csv_arg,
                          default=[])
        self.add_argument("--trend-lead",
                          help="Time steps in the future for linear trends",
                          type=int_or_list_arg,
                          default=93)
        return self

    def add_reference_arg(self):
        self.add_argument("-r",
                          "--ref",
                          help="Reference loader for normalisations etc",
                          default=None,
                          type=str)
        return self

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


def process_date_args(args: object) -> dict:
    """

    :param args:
    :return:
    """
    dates = dict(train=[], val=[], test=[])

    for dataset in ("train", "val", "test"):
        dataset_dates = collections.deque()

        for i, period_start in \
                enumerate(getattr(args, "{}_start".format(dataset))):
            period_end = getattr(args, "{}_end".format(dataset))[i]
            dataset_dates += [
                pd.to_datetime(date).date()
                for date in pd.date_range(period_start, period_end, freq="D")
            ]
        logging.info("Got {} dates for {}".format(len(dataset_dates), dataset))

        dates[dataset] = sorted(list(dataset_dates))
    return dates

