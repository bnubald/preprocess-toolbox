from download_toolbox.interface import get_dataset_config_implementation

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

    era5 = implementation(ds_config,
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
    era5.process()

