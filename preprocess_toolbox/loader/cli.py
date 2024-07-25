import argparse
import logging
import os

import orjson

from preprocess_toolbox.cli import BaseArgParser


class LoaderArgParser(BaseArgParser):
    """An ArgumentParser specialised to support forecast plot arguments

    The 'allow_*' methods return self to permit method chaining.

    :param suppress_logs:
    """

    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.add_argument("name",
                          type=str)

    def add_prefix(self):
        self.add_argument("-p",
                          "--prefix",
                          type=str,
                          default="loader")
        return self

    def add_configurations(self):
        self.add_argument("configurations",
                          type=argparse.FileType("r"),
                          nargs="+")
        return self


def init_loader():
    args = (LoaderArgParser().
            add_prefix().
            add_configurations().
            parse_args())
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

    destination_filename = "{}.{}.json".format(args.prefix, args.name)

    if not os.path.exists(destination_filename):
        with open(destination_filename, "w") as fh:
            fh.write(orjson.dumps(data).decode())
    else:
        raise FileExistsError("It's pretty pointless calling init on an existing configuration, "
                              "perhaps delete the file first and go for it")
