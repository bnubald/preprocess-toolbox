from download_toolbox.interface import get_dataset_config_implementation

from preprocess_toolbox.cli import ProcessingArgParser, process_split_args
from preprocess_toolbox.processor import NormalisingChannelProcessor


def process_dataset():
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
    era5 = NormalisingChannelProcessor(ds_config,
                                       args.abs,
                                       args.anom,
                                       splits,
                                       anom_clim_splits=args.processing_splits,
                                       identifier=args.destination_id,
                                       linear_trends=args.trends,
                                       linear_trend_steps=args.trend_lead,
                                       normalisation_splits=args.processing_splits,
                                       parallel_opens=args.parallel_opens or False,
                                       ref_procdir=args.ref)
    era5.process()

