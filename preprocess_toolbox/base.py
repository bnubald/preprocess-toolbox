from abc import abstractmethod

import logging
import os

import dask.array
import numpy as np
from dateutil.relativedelta import relativedelta
from pprint import pformat

from download_toolbox.config import Configuration
from download_toolbox.interface import DataCollection, DatasetConfig, Frequency


class ProcessingError(RuntimeError):
    pass


class Processor(DataCollection):
    """An abstract base class for data processing classes.

    Provides methods for initialising source data from download-toolbox defined
    configurations, process the data, and saving the processed data to normalised netCDF files.

    TODO: the majority of actual data processing, for the moment, is being isolated in the
     child implementation of NormalisingChannelProcessor whilst I work out what is going
     on regarding the relevant processing and the inheritance hierarchy that support future,
     diverse implementations for alternative data types not based on xarray

    """

    def __init__(self,
                 dataset_config: DatasetConfig,
                 absolute_vars: list,
                 anomoly_vars: list,
                 splits: dict,
                 identifier: str,
                 # TODO: anomoly vars should instead be in the NormalisingChannelProcessor?
                 #  need to have a solid think about the hierarchy and how it allows for expansion
                 #  and sensible decomposition of functionality for the future: much of this
                 #  functionality wouldn't belong in, say, a satellite image processor (channels would though!)
                 anom_clim_splits: list = None,
                 base_path: os.PathLike = os.path.join(".", "processed"),
                 dtype: np.typecodes = np.float32,
                 lag_time: int = 0,
                 lead_time: int = 3,
                 linear_trends: list = None,
                 linear_trend_steps: int = 7,
                 minmax: bool = True,
                 no_normalise: tuple = None,
                 normalisation_splits: list = None,
                 parallel_opens: bool = False,
                 ref_procdir: os.PathLike = None,
                 update_key: str = None,
                 **kwargs) -> None:
        super().__init__(base_path=base_path,
                         config_type="processed",
                         identifier=identifier,
                         path_components=[])

        self.config.directory = "."

        self._abs_vars = absolute_vars if absolute_vars else []
        self._anom_clim_splits = [] if anom_clim_splits is None else anom_clim_splits
        self._anom_vars = anomoly_vars if anomoly_vars else []
        self._dtype = dtype
        self._lag_time = lag_time
        self._lead_time = lead_time
        self._linear_trends = linear_trends

        if type(linear_trend_steps) is int:
            logging.debug(
                "Setting range for linear trend steps based on {}".format(
                    linear_trend_steps))
            self._linear_trend_steps = list(range(1, linear_trend_steps + 1))
        else:
            self._linear_trend_steps = [int(el) for el in linear_trend_steps]
        # Dates = collections.namedtuple("Dates", list(splits.keys()))
        # self._dates = Dates(**{split: split_dates for split, split_dates in splits.items()})

        # TODO: this should be coming from the datasetconfigs
        self._missing_dates = list()
        self._no_normalise = no_normalise if no_normalise is not None else tuple()
        self._normalise = self._normalise_array_mean \
            if not minmax else self._normalise_array_scaling
        self._normalisation_splits = [] if normalisation_splits is None else normalisation_splits
        self._parallel = parallel_opens
        self._processed_files = dict()
        self._refdir = ref_procdir
        # TODO: splits -> { dates, sources }, but currently sources are separate...
        self._splits = splits
        self._source_files = dict()
        self._update_key = self.identifier if not update_key else update_key

        self._init_source_data(dataset_config)

    def _init_source_data(self,
                          ds_config: DatasetConfig) -> None:
        """

        :return:
        """

        for split in self._splits.keys():
            dates = sorted(self._splits[split])

            if dates:
                logging.info("Processing {} dates for {} category".format(len(dates), split))
            else:
                logging.info("No {} dates for this processor".format(split))
                continue

            # Calculating lag dates that aren't already accounted for
            if self._lag_time > 0:
                logging.info("Including lag of {} {}s".format(self._lag_time, ds_config.frequency.attribute))

                additional_lag_dates = []

                for date in dates:
                    for time in range(self._lag_time):
                        attrs = {"{}s".format(ds_config.frequency.attribute): time + 1}
                        lag_date = date - relativedelta(**attrs)
                        if lag_date not in dates:
                            additional_lag_dates.append(lag_date)
                dates += list(set(additional_lag_dates))

            if self._lead_time > 0:
                logging.info("Including lead of {} days".format(self._lead_time))

                additional_lead_dates = []

                for date in dates:
                    for time in range(self._lead_time):
                        attrs = {"{}s".format(ds_config.frequency.attribute): time + 1}
                        lead_date = date + relativedelta(**attrs)
                        if lead_date not in dates:
                            additional_lead_dates.append(lead_date)
                dates += list(set(additional_lead_dates))

            self._source_files[split] = {var_config.name: sorted(ds_config.var_filepaths(var_config, dates))
                                         for var_config in ds_config.variables}

        for split in self._source_files:
            for var_name, var_files in self._source_files[split].items():
                for var_file in var_files:
                    if not os.path.exists(var_file):
                        logging.warning("{} does not exist, no inclusion in relevant samples")
                logging.info("Got {} files for {}:{}".format(len(var_files), split, var_name))

        logging.debug(pformat(self._source_files))

    def _normalise_array_mean(self, var_name: str, da: object):
        """
        Using the *training* data only, compute the mean and
        standard deviation of the input raw satellite DataArray (`da`)
        and return a normalised version. If minmax=True,
        instead normalise to lie between min and max of the elements of `array`.

        If min, max, mean, or std are given values other than None,
        those values are used rather than being computed from the training
        months.

        :param var_name:
        :param da:
        :return:
        """

        if self._refdir:
            logging.info("Using alternate processing directory {} for "
                         "mean".format(self._refdir))
            proc_dir = os.path.join(self._refdir, "normalisation.mean")
        else:
            proc_dir = self.get_data_var_folder("normalisation.mean")

        mean_path = os.path.join(proc_dir, "{}".format(var_name))

        if os.path.exists(mean_path):
            logging.debug(
                "Loading norm-average mean-std from {}".format(mean_path))
            mean, std = tuple([
                self._dtype(el)
                for el in open(mean_path, "r").read().split(",")
            ])
        elif len(self.norm_split_dates) > 0:
            logging.debug("Generating norm-average mean-std from {} training "
                          "dates".format(len(self.norm_split_dates)))
            norm_samples = da.sel(time=self.norm_split_dates).data
            norm_samples = norm_samples.ravel()

            mean, std = Processor.mean_and_std(norm_samples)
        else:
            raise RuntimeError("Either a normalisation file or normalisation split dates "
                               "must be supplied")

        new_da = (da - mean) / std

        if not self._refdir:
            open(mean_path, "w").write(",".join([str(f) for f in [float(mean), float(std)]]))
        return new_da

    def _normalise_array_scaling(self, var_name: str, da: object):
        """

        :param var_name:
        :param da:
        :return:
        """
        if self._refdir:
            logging.info("Using alternate processing directory {} for "
                         "scaling".format(self._refdir))
            proc_dir = os.path.join(self._refdir, "normalisation.scale")
        else:
            proc_dir = self.get_data_var_folder("normalisation.scale")

        scale_path = os.path.join(proc_dir, "{}".format(var_name))

        if os.path.exists(scale_path):
            logging.debug(
                "Loading norm-scaling min-max from {}".format(scale_path))
            minimum, maximum = tuple([
                self._dtype(el)
                for el in open(scale_path, "r").read().split(",")
            ])
        elif self.norm_split_dates:
            logging.debug("Generating norm-scaling min-max from {} training "
                          "dates".format(len(self.norm_split_dates)))

            norm_samples = da.sel(time=self.norm_split_dates).data
            norm_samples = norm_samples.ravel()

            minimum = dask.array.nanmin(norm_samples).astype(self._dtype)
            maximum = dask.array.nanmax(norm_samples).astype(self._dtype)
        else:
            raise RuntimeError("Either a normalisation file or training data "
                               "must be supplied")

        new_da = (da - minimum) / (maximum - minimum)
        if not self._refdir:
            open(scale_path, "w").write(",".join([str(f) for f in [float(minimum), float(maximum)]]))
        return new_da

    def get_data_var_folder(self,
                            var_name: str,
                            append: object = None,
                            missing_error: bool = False) -> os.PathLike:
        """Returns the path for a specific data variable.

        Appends additional folders to the path if specified in the `append` parameter.

        :param var_name: The data variable.
        :param append: Additional folders to append to the path.
        :param missing_error: Flag to specify if missing directories should be treated as an error.

        :return str: The path for the specific data variable.
        """
        if not append:
            append = []

        data_var_path = os.path.join(self.path, *[var_name, *append])

        if not os.path.exists(data_var_path):
            if not missing_error:
                os.makedirs(data_var_path, exist_ok=True)
            else:
                raise OSError("Directory {} is missing and this is "
                              "flagged as an error!".format(data_var_path))

        return data_var_path

    @staticmethod
    def mean_and_std(array: object):
        """
        Return the mean and standard deviation of an array-like object (intended
        use case is for normalising a raw satellite data array based on a list
        of samples used for training).
        :param array:
        :return:
        """

        mean = dask.array.nanmean(array)
        std = dask.array.nanstd(array)

        logging.info("Mean: {:.3f}, std: {:.3f}".format(
            mean.item(), std.item()))

        return mean, std

    @abstractmethod
    def process(self):
        pass

    @property
    def anom_split_dates(self) -> list:
        # TODO: functools.cached_property, though slightly odd behaviour re. write-ability
        return [date
                for clim_split in self._anom_clim_splits
                for date in self._splits[clim_split]]

    @property
    def lead_time(self) -> int:
        """The lead time used in the data processing."""
        return self._lead_time

    @property
    def missing_dates(self):
        return self._missing_dates

    @missing_dates.setter
    def missing_dates(self, arr):
        self._missing_dates = arr

    @property
    def norm_split_dates(self):
        # TODO: functools.cached_property, though slightly odd behaviour re. write-ability
        return [date
                for clim_split in self._normalisation_splits
                for date in self._splits[clim_split]]

    @property
    def processed_files(self) -> dict:
        """A dict with the processed files organised by variable name."""
        return self._processed_files

    @property
    def source_files(self) -> dict:
        return self._source_files

    @property
    def splits(self) -> object:
        """The dates used for training, validation, and testing in this class as a named collections.tuple."""
        return self._splits

