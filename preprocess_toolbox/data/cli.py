import logging
import shutil

from preprocess_toolbox.cli import ProcessingArgParser
from preprocess_toolbox.data.time import missing_dates
from download_toolbox.interface import get_dataset_implementation


def missing_time():
    args = ProcessingArgParser().add_destination().parse_args()
    ds = get_dataset_implementation(args.source)

    if args.destination is not None:
        logging.debug("Got destination {}".format(args.destination))
        if not os.path.exists(args.destination):
            shutil.copy()
        else:
            raise RuntimeError("{} already exists, please remove or think about destructive processing".format(args.destination))
    print(args)
    print(dir(ds))


def missing_spatial():
    args = ProcessingArgParser().parse_args()


def regrid():
    args = ProcessingArgParser().parse_args()


def rotate():
    args = ProcessingArgParser().parse_args()

