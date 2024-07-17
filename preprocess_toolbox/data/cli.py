import argparse
import logging

from functools import wraps

from preprocess_toolbox.cli import ProcessingArgParser
from preprocess_toolbox.data.process import regrid_dataset, rotate_dataset
from preprocess_toolbox.data.spatial import spatial_interpolation
from preprocess_toolbox.data.time import process_missing_dates
from download_toolbox.interface import get_dataset_config_implementation


def init_dataset(args):
    ds_config = get_dataset_config_implementation(args.source)

    if args.destination_id is not None:
        ds_config.copy_to(args.destination_id)

    if args.var_names is None:
        raise RuntimeError("No variable names have been provided")

    ds = ds_config.get_dataset(args.var_names)
    return ds, ds_config


def missing_time():
    args = ProcessingArgParser().add_destination_arg().add_var_name_arg().parse_args()
    ds, ds_config = init_dataset(args)

    for var_name in args.var_names:
        logging.info("Processing missing dates for {}".format(var_name))
        ds = process_missing_dates(ds, ds_config, var_name)

    ds_config.save_data_for_config(source_ds=ds)


def missing_spatial():
    args = ProcessingArgParser(suppress_logs=["PIL"]).add_destination_arg().add_var_name_arg().parse_args()
    ds, ds_config = init_dataset(args)

    for var_name in args.var_names:
        logging.info("Processing missing dates for {}".format(var_name))
        ds[var_name] = spatial_interpolation(getattr(ds, var_name),
                                             ds_config,
                                             None,
                                             save_comparison_fig=True)

    ds_config.save_data_for_config(source_ds=ds)


def regrid():
    args = ProcessingArgParser().add_ref_ds_arg().add_destination_arg().parse_args()
    ds, ds_config = init_dataset(args)
    ref_ds = get_dataset_config_implementation(args.reference).get_dataset()
    regrid_dataset(ref_ds, ds)


def rotate():
    args = ProcessingArgParser().add_ref_ds_arg().parse_args()
    ds, ds_config = init_dataset(args)
    ref_ds = get_dataset_config_implementation(args.reference).get_dataset()
    rotate_dataset(ref_ds, ds)



