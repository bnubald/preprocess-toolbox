import argparse
import logging

from functools import wraps

from preprocess_toolbox.cli import ProcessingArgParser
from preprocess_toolbox.data.spatial import spatial_interpolation
from preprocess_toolbox.data.time import process_missing_dates
from preprocess_toolbox.dataset import clone_dataset
from download_toolbox.interface import get_dataset_config_implementation


def init_downloaded_dataset(args):
    configuration = args.source \
        if args.destination_id is None else clone_dataset(args.source, args.destination_id)
    ds_config = get_dataset_config_implementation(configuration)

    if args.var_names is None:
        raise RuntimeError("No variable names have been provided")

    ds = ds_config.get_dataset(args.var_names)
    return ds, ds_config


def save_downloaded_dataset(ds, ds_config):
    pass


def missing_time():
    args = ProcessingArgParser().add_destination_arg().add_var_name_arg().parse_args()
    ds, ds_config = init_downloaded_dataset(args)

    for var_name in args.var_names:
        logging.info("Processing missing dates for {}".format(var_name))
        ds = process_missing_dates(ds, ds_config, var_name)

    save_downloaded_dataset(ds, ds_config)


def missing_spatial():
    args = ProcessingArgParser(suppress_logs=["PIL"]).add_destination_arg().add_var_name_arg().parse_args()
    ds, ds_config = init_downloaded_dataset(args)

    for var_name in args.var_names:
        logging.info("Processing missing dates for {}".format(var_name))
        ds[var_name] = spatial_interpolation(getattr(ds, var_name),
                                             ds_config,
                                             None,
                                             save_comparison_fig=True)

    save_downloaded_dataset(ds, ds_config)


def regrid():
    args = ProcessingArgParser().parse_args()


def rotate():
    args = ProcessingArgParser().parse_args()

