from download_toolbox.base import DatasetConfig

from preprocess_toolbox.metadata.meta import MetaPreProcessor
from preprocess_toolbox.metadata.mask import Masks
from preprocess_toolbox.cli import ProcessingArgParser


def date():
    args = ProcessingArgParser().parse_args()

    MetaPreProcessor(args.name,
                     args.dataset).process()


def mask():
    args = ProcessingArgParser().parse_args()
    ds = DatasetConfig.load_dataset(args.dataset)
    masks = Masks(args.name)


