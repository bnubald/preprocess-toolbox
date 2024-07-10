from abc import abstractmethod, ABCMeta

import collections
import datetime as dt
import glob
import logging
import os
import re

import pandas as pd

from download_toolbox.base import DataCollection


class GenerateProcessor(DataCollection):
    """Abstract base class for a generator."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def generate(self):
        """Abstract generate method for this generator: Must be implemented by subclasses."""

        raise NotImplementedError("{}.generate is abstract".format(
            __class__.__name__))


class DataProcessor(DataCollection):
    """An abstract base class for data processing classes.

    Provides methods for initialising source data, processing the data, and
        saving the processed data to standard netCDF files.

    Attributes:
        _file_filters: List of file filters to exclude certain files during data processing.
        _lead_time: Forecast/lead time used in the data processing.
        source_data: Path to the source data directory.
        _var_files: Dictionary storing variable files organised by variable name.
        _processed_files: Dictionary storing the processed files organised by variable name.
        _dates: Named tuple that stores the dates used for training, validation, and testing.
    """


