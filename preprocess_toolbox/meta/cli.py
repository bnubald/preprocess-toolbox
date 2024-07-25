import argparse
import logging
import os

import orjson

from download_toolbox.interface import get_dataset_config_implementation

from preprocess_toolbox.loader.cli import LoaderArgParser
from preprocess_toolbox.loader.utils import get_processed_path_for_dataset, update_config
from preprocess_toolbox.utils import get_implementation


class MetaArgParser(LoaderArgParser):
    def __init__(self):
        super().__init__()
        self.add_argument("ground_truth_dataset")

    def add_channel(self):
        self.add_argument("channel_name")
        self.add_argument("implementation")
        return self


def channel():
    args = (MetaArgParser().
            add_channel().
            add_prefix().
            parse_args())

    proc_impl = get_implementation(args.implementation)
    ds_config = get_dataset_config_implementation(args.ground_truth_dataset)
    processor = proc_impl(ds_config,
                          [args.channel_name,],
                          args.channel_name,
                          base_path=get_processed_path_for_dataset(args.name, args.ground_truth_dataset))
    processor.process()
    cfg = processor.get_config()
    update_config(args.name, "channels", cfg)


def mask():
    args = (MetaArgParser().
            add_channel().
            add_prefix().
            parse_args())
    proc_impl = get_implementation(args.implementation)
    ds_config = get_dataset_config_implementation(args.ground_truth_dataset)
    filenames = proc_impl(ds_config,
                          args.channel_name,
                          get_processed_path_for_dataset(args.name, args.ground_truth_dataset))
    update_config(args.name, "masks", {
        args.channel_name: filenames
    })
