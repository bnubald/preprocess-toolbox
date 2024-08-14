import argparse
import logging
import os

import orjson

from preprocess_toolbox.cli import BaseArgParser
from preprocess_toolbox.loader.utils import update_config
from preprocess_toolbox.utils import get_implementation


from download_toolbox.interface import get_dataset_config_implementation


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


class MetaArgParser(LoaderArgParser):
    def __init__(self):
        super().__init__()
        self.add_argument("ground_truth_dataset")

    def add_channel(self):
        self.add_argument("channel_name")
        self.add_argument("implementation")
        return self

    def add_property(self):
        self.add_argument("-p", "--property",
                          type=str, default=None)
        return self


def create():
    args = (LoaderArgParser().
            add_prefix().
            parse_args())

    data = dict(
        identifier=args.name,
        filenames=dict(),
        sources=dict(),
        masks=dict(),
        channels=dict(),
    )
    destination_filename = "{}.{}.json".format(args.prefix, args.name)

    if not os.path.exists(destination_filename):
        with open(destination_filename, "w") as fh:
            fh.write(orjson.dumps(data, option=orjson.OPT_INDENT_2).decode())
        logging.info("Created a configuration {} to build on".format(destination_filename))
    else:
        raise FileExistsError("It's pretty pointless calling init on an existing configuration, "
                              "perhaps delete the file first and go for it")


def add_processed():
    args = (LoaderArgParser().
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

    update_config(args.name, "filenames", filenames)
    update_config(args.name, "sources", cfgs)


def get_channel_info_from_processor(cfg_segment: str):
    args = (MetaArgParser().
            add_channel().
            parse_args())

    proc_impl = get_implementation(args.implementation)
    ds_config = get_dataset_config_implementation(args.ground_truth_dataset)
    processor = proc_impl(ds_config,
                          [args.channel_name,],
                          args.channel_name)
    processor.process()
    cfg = processor.get_config()
    update_config(args.name, cfg_segment, cfg)


def add_channel():
    get_channel_info_from_processor("channels")


def add_mask():
    get_channel_info_from_processor("masks")
