import argparse
import copy
import logging
import os

import orjson


class LoaderArgParser(argparse.ArgumentParser):
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

        self.add_argument("name",
                          type=str)
        self.add_argument("configurations",
                          type=argparse.FileType("r"),
                          nargs="+")

        self.add_argument("-v",
                          "--verbose",
                          action="store_true",
                          default=False)

    def parse_args(self, *args, **kwargs):
        args = super().parse_args(*args, **kwargs)

        loglevel = logging.DEBUG if args.verbose else logging.INFO
        logging.basicConfig(level=loglevel)
        logging.getLogger().setLevel(loglevel)
        logging.getLogger("matplotlib").setLevel(logging.WARNING)

        return args


def init_loader():
    args = LoaderArgParser().parse_args()
    cfgs = dict()
    filenames = dict()

    for fh in args.configurations:
        logging.info("Configuration {} being loaded".format(fh.name))
        cfg_data = orjson.loads(fh.read())

        if "data" not in cfg_data:
            raise KeyError("There's no data element in {}, that's not right!".format(fh.name))
        _, name, _ = fh.name.split(".")
        cfgs[name] = cfg_data["data"]
        filenames[name] = fh.name
        fh.close()

    data = dict(
        filenames=filenames,
        sources=cfgs
    )

    with open("loader.{}.json".format(args.name), "w") as fh:
        fh.write(orjson.dumps(data).decode())
