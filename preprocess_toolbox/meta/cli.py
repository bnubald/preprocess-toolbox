import argparse
import os

from download_toolbox.interface import get_dataset_config_implementation

from preprocess_toolbox.loader.cli import LoaderArgParser
from preprocess_toolbox.loader.utils import update_config
from preprocess_toolbox.utils import get_implementation


class MetaArgParser(LoaderArgParser):
    def __init__(self):
        super().__init__()
        self.add_argument("ground-truth-dataset", type=str)

    def add_channel(self):
        self.add_argument("channel-name")
        self.add_argument("implementation")
        return self


def channel():
    args = (MetaArgParser().
            add_channel().
            add_prefix().
            parse_args())
    loader_configuration = "{}.{}.json".format(args.prefix, args.name)
    proc_impl = get_implementation(args.implementation)
    ds_config = get_dataset_config_implementation(args.ground_truth_dataset)
    processor = proc_impl(ds_config,
                          [args.channel_name,],
                          args.channel_name)
    processor.process()
    cfg = processor.get_config()
    update_config(loader_configuration, args.channel_name, cfg)


def mask():
    args = (MetaArgParser().
            add_channel().
            add_prefix().
            parse_args())
    loader_configuration = "{}.{}.json".format(args.prefix, args.name)
    proc_impl = get_implementation(args.implementation)
    ds_config = get_dataset_config_implementation(args.ground_truth_dataset)
    filenames = proc_impl(ds_config, args.channel_name)
    update_config(loader_configuration, args.channel_name, dict(
        masks={
            args.channel_name: filenames
        }
    ))
