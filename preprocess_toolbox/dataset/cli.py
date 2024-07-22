from download_toolbox.interface import get_dataset_config_implementation

from preprocess_toolbox.cli import ProcessingArgParser, process_split_args
from preprocess_toolbox.dataset.datasets import SICPreProcessor, ERA5PreProcessor


def amsr2():
    args = ProcessingArgParser().add_destination_arg().parse_args()


def cmip6():
    args = (ProcessingArgParser().
            add_destination_arg().
            add_split_args().
            add_var_args()).parse_args()

    cmip = IceNetCMIPPreProcessor(
        args.source,
        args.member,
        args.abs,
        args.anom,
        args.name,
        dates["train"],
        dates["val"],
        dates["test"],
        linear_trends=args.trends,
        linear_trend_days=args.trend_lead,
        north=args.hemisphere == "north",
        parallel_opens=args.parallel_opens,
        ref_procdir=args.ref,
        south=args.hemisphere == "south",
        update_key=args.update_key,
    )
    cmip.init_source_data(lag_days=args.lag,)
    cmip.process()


def era5():
    args = (ProcessingArgParser().
            add_concurrency().
            add_destination_arg().
            add_reference_arg().
            add_split_args().
            add_trend_args().
            add_var_args()).parse_args()
    ds_config = get_dataset_config_implementation(args.source)
    splits = process_split_args(args, frequency=ds_config.frequency)

    # TODO: Icenet pipeline usage if we don't need dedicated implementations!!!
    era5 = ERA5PreProcessor(ds_config,
                            args.abs,
                            args.anom,
                            splits,
                            anom_clim_splits=["train",],
                            identifier=args.destination_id,
                            linear_trends=args.trends,
                            linear_trend_steps=args.trend_lead,
                            normalisation_splits=["train",],
                            parallel_opens=args.parallel_opens or False,
                            ref_procdir=args.ref)
    era5.process()


def osisaf():
    args = (ProcessingArgParser().
            add_concurrency().
            add_destination_arg().
            add_reference_arg().
            add_split_args().
            add_trend_args().
            add_var_args()).parse_args()
    ds_config = get_dataset_config_implementation(args.source)
    splits = process_split_args(args, frequency=ds_config.frequency)

    # TODO: !!! Not sure we should even need dedicated implementations!?
    osi = SICPreProcessor(ds_config,
                          args.abs,
                          args.anom,
                          splits,
                          identifier=args.destination_id,
                          linear_trends=args.trends,
                          linear_trend_steps=args.trend_lead,
                          normalisation_splits=["train", "val"],
                          parallel_opens=args.parallel_opens or False,
                          ref_procdir=args.ref)

    osi.process()
