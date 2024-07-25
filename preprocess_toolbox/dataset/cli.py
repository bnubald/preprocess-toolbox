import argparse
import logging

from functools import wraps

from download_toolbox.interface import get_dataset_config_implementation

from preprocess_toolbox.dataset.process import regrid_dataset, rotate_dataset
from preprocess_toolbox.dataset.spatial import spatial_interpolation
from preprocess_toolbox.dataset.time import process_missing_dates
from preprocess_toolbox.cli import ProcessingArgParser, process_split_args
from preprocess_toolbox.processor import NormalisingChannelProcessor
from preprocess_toolbox.utils import get_implementation


def process_dataset():
    args = (ProcessingArgParser().
            add_concurrency().
            add_destination().
            add_implementation().
            add_reference().
            add_splits().
            add_trends().
            add_vars()).parse_args()
    ds_config = get_dataset_config_implementation(args.source)
    splits = process_split_args(args, frequency=ds_config.frequency)

    implementation = NormalisingChannelProcessor if args.implementation is None else get_implementation(args.implementation)

    proc = implementation(ds_config,
                          args.anom,
                          splits,
                          args.abs,
                          anom_clim_splits=args.processing_splits,
                          identifier=args.destination_id,
                          linear_trends=args.trends,
                          linear_trend_steps=args.trend_lead,
                          normalisation_splits=args.processing_splits,
                          parallel_opens=args.parallel_opens or False,
                          ref_procdir=args.ref)
    proc.process()

def init_dataset(args):
    ds_config = get_dataset_config_implementation(args.source)

    if args.destination_id is not None:
        ds_config.copy_to(args.destination_id)

    var_names = None if "var_names" not in args else args.var_names
    ds = ds_config.get_dataset(var_names)
    return ds, ds_config


def missing_time():
    args = ProcessingArgParser().add_destination().add_var_name().parse_args()
    ds, ds_config = init_dataset(args)

    for var_name in args.var_names:
        logging.info("Processing missing dates for {}".format(var_name))
        ds = process_missing_dates(ds, ds_config, var_name)

    ds_config.save_data_for_config(source_ds=ds)


def missing_spatial():
    args = ProcessingArgParser(suppress_logs=["PIL"]).add_destination().add_var_name().parse_args()
    ds, ds_config = init_dataset(args)

    for var_name in args.var_names:
        logging.info("Processing missing dates for {}".format(var_name))
        ds[var_name] = spatial_interpolation(getattr(ds, var_name),
                                             ds_config,
                                             None,
                                             save_comparison_fig=True)

    ds_config.save_data_for_config(source_ds=ds)


def regrid():
    args = ProcessingArgParser().add_ref_ds().add_destination().parse_args()
    ds, ds_config = init_dataset(args)

    regrid_dataset(args.reference, ds_config)
    ds_config.save_config()


def rotate():
    args = ProcessingArgParser().add_ref_ds().add_destination().add_var_name().parse_args()
    ds, ds_config = init_dataset(args)

    rotate_dataset(args.reference, ds_config)
    ds_config.save_config()

