import logging
import os

from dateutil.relativedelta import relativedelta
from pprint import pformat

import dask
import dask.array
import numpy as np
import orjson
import pandas as pd
import xarray as xr

from preprocess_toolbox.base import Processor, ProcessingError
from preprocess_toolbox.models import linear_trend_forecast

from download_toolbox.interface import DatasetConfig, Frequency


class NormalisingChannelProcessor(Processor):
    """

    TODO: at the moment this class implementation is solely intended to break apart
     the heavier lifting happening via xarray, for the most part

    """

    def __init__(self,
                 dataset_config: DatasetConfig,
                 anomoly_vars: list,
                 splits: dict,
                 *args,
                 anom_clim_splits: list = None,
                 clim_frequency: Frequency = Frequency.MONTH,
                 init_source: bool = True,
                 lag_time: int = 1,
                 lead_time: int = 3,
                 linear_trends: list = None,
                 linear_trend_steps: int = 7,
                 minmax: bool = True,
                 no_normalise: tuple = None,
                 normalisation_splits: list = None,
                 parallel_opens: bool = True,
                 ref_procdir: os.PathLike = None,
                 **kwargs):
        """

        Args:
            dataset_config:
            anomoly_vars:
            splits:
            *args:
            anom_clim_splits:
            clim_frequency:
            lag_time:
            lead_time:
            linear_trends:
            linear_trend_steps:
            minmax:
            no_normalise:
            normalisation_splits:
            parallel_opens:
            ref_procdir:
            **kwargs:
        """
        super().__init__(dataset_config, *args, **kwargs)

        if clim_frequency != Frequency.MONTH:
            raise NotImplementedError("We only generate climatologies at a monthly resolution, "
                                      "this needs implementation")

        self._anom_clim_splits = [] if anom_clim_splits is None else anom_clim_splits
        self._anom_vars = anomoly_vars if anomoly_vars else []
        self._dataset_config = dataset_config.config_file
        # This is important to inherit from the dataset and carry forward, it has a lot of downstream impact
        # TODO: time and spatial information validation - if the source changes what do we do!?
        self._frequency = dataset_config.frequency
        self._lag_time = lag_time
        self._lead_time = lead_time
        self._linear_trends = linear_trends
        # TODO: spatial information has been overlooked so far, but needs to carry forward and validate
        self._location = dataset_config.location

        if type(linear_trend_steps) is int:
            logging.debug(
                "Setting range for linear trend steps based on {}".format(
                    linear_trend_steps))
            self._linear_trend_steps = list(range(1, linear_trend_steps + 1))
        else:
            self._linear_trend_steps = [int(el) for el in linear_trend_steps]
        # Dates = collections.namedtuple("Dates", list(splits.keys()))
        # self._dates = Dates(**{split: split_dates for split, split_dates in splits.items()})

        self._no_normalise = no_normalise if no_normalise is not None else tuple()
        self._normalise = self._normalise_array_mean \
            if not minmax else self._normalise_array_scaling
        self._normalisation_splits = [] if normalisation_splits is None else normalisation_splits
        self._parallel = parallel_opens
        self._refdir = ref_procdir
        # TODO: splits -> { dates, sources }, but currently sources are separate...
        self._splits = splits
        self._source_files = dict()

        if init_source:
            self._init_source_data(dataset_config)

    def _build_linear_trend_da(self,
                               input_da: object,
                               var_name: str,
                               max_years: int = 35,
                               ref_da: object = None):
        """
        Construct a DataArray `linear_trend_da` containing the linear trend
        forecasts based on the input DataArray `input_da`.

        :param input_da:
        :param var_name:
        :param max_years:
        :param ref_da:
        :return:
        """

        if ref_da is None:
            ref_da = input_da
        data_dates = sorted([pd.Timestamp(date) for date in input_da.time.values])

        trend_dates = set()
        trend_steps = max(self._linear_trend_steps)

        extract_date_map = dict(
            year=lambda dt: "{}".format(dt.year),
            month=lambda dt: "{}-{}".format(dt.year, dt.month),
            day=lambda dt: "{}-{}-{}".format(dt.year, dt.month, dt.day),
            hour=lambda dt: "{}-{}-{}T{}".format(dt.year, dt.month, dt.day, dt.hour),
        )
        if self._frequency.attribute == "hour":
            raise NotImplementedError("Hour based linear trends are not implemented yet")

        trend_range = pd.date_range(pd.to_datetime(extract_date_map[self._frequency.attribute](data_dates[0])),
                                    pd.to_datetime(extract_date_map[self._frequency.attribute](data_dates[-1])) +
                                    relativedelta(**{"{}s".format(self._frequency.attribute): trend_steps + 1}),
                                    freq=self._frequency.freq)

        logging.info("Generating trend data up to {} steps ahead for {} dates".
                     format(trend_steps, len(data_dates)))

        for dat_date in data_dates:
            base_idx = list(trend_range).index(dat_date)
            trend_dates = trend_dates.union([
                trend_range[base_idx + idx]
                for idx in self._linear_trend_steps
            ])

        trend_dates = list(sorted(trend_dates))
        logging.info("Generating {} trend dates".format(len(trend_dates)))

        linear_trend_da = \
            xr.broadcast(input_da, xr.DataArray(trend_range, dims="time"))[0]
        linear_trend_da = linear_trend_da.sel(time=trend_dates)
        linear_trend_da.data = dask.array.zeros(linear_trend_da.shape)

        # TODO: what are we applying this for here?
        # land_mask = Masks(north=self.north, south=self.south).get_land_mask()

        # Could use shelve, but more likely we'll run into concurrency issues
        # pickleshare might be an option but a little over-engineery
        trend_cache_path = os.path.join(self.path,
                                        "{}_linear_trend.nc".format(var_name))
        trend_cache = linear_trend_da.copy()
        trend_cache.data = dask.array.full_like(linear_trend_da.data, np.nan)

        if os.path.exists(trend_cache_path):
            trend_cache = xr.open_dataarray(trend_cache_path)
            logging.info("Loaded {} entries from {}".format(
                len(trend_cache.time), trend_cache_path))

        def data_selector(da, processing_date, missing_dates=tuple()):
            target_date = pd.to_datetime(processing_date)

            # TODO: We're assuming the linear trend as a day-res year long application
            # TODO: I've hacked a leap year in for the mo, but this should be using loc, a simplified clause and isel
            #  such as by starting with date_da = da.loc[target_date:]
            date_da = da[(da.time['time.month'] == target_date.month) &
                         ((da.time['time.day'] == target_date.day) |
                          (da.time["time.day"] == target_date.day - 1)) &
                         (da.time <= target_date) &
                         ~da.time.isin(missing_dates)].\
                isel(time=slice(0, max_years))
            return date_da

        for forecast_date in sorted(trend_dates, reverse=True):
            if not trend_cache.sel(time=forecast_date).isnull().all():
                output_map = trend_cache.sel(time=forecast_date)
            else:
                output_map = linear_trend_forecast(
                    data_selector,
                    forecast_date,
                    ref_da,
                    None,  # masks
                    missing_dates=[], # TODO: self._missing_dates,
                    shape=ref_da.isel(time=0).shape, # shape
                )

            linear_trend_da.loc[dict(time=forecast_date)] = output_map

        logging.info("Writing new trend cache for {}".format(var_name))
        trend_cache.close()
        linear_trend_da = linear_trend_da.rename("{}_linear_trend".format(var_name))
        self.save_processed_file("{}_linear_trend".format(var_name),
                                 "{}_linear_trend.nc".format(var_name),
                                 linear_trend_da)

        return linear_trend_da

    def _init_source_data(self,
                          ds_config: DatasetConfig) -> None:
        """

        :return:
        """

        drop_dates = dict()

        for split in self._splits.keys():
            dates = sorted(self._splits[split])
            drop_dates[split] = list()

            if dates:
                logging.info("Processing {} dates for {} category".format(len(dates), split))
            else:
                logging.info("No {} dates for this processor".format(split))
                continue

            # TODO: two repeated check blocks for lag and lead, this can be generalised
            # Calculating lag dates that aren't already accounted for
            if self._lag_time > 0:
                logging.info("Including lag of {} {}s".format(self._lag_time, ds_config.frequency.attribute))

                additional_lag_dates = []

                for date in dates:
                    for time in range(self._lag_time):
                        attrs = {"{}s".format(ds_config.frequency.attribute): time + 1}
                        lag_date = date - relativedelta(**attrs)

                        if lag_date not in dates:
                            if all([os.path.exists(ds_config.var_filepath(var_config, [lag_date]))
                                    for var_config in ds_config.variables]):
                                # We only add these dates into the mix if all necessary files exist
                                additional_lag_dates.append(lag_date)
                            else:
                                # Otherwise, warn that the lag data means this is being dropped
                                logging.warning("{} will be dropped from {} due to missing lag data {}".
                                                format(date, split, lag_date))
                                drop_dates[split].append(date)

                dates += list(set(additional_lag_dates))

            if self._lead_time > 0:
                logging.info("Including lead of {} {}s".format(self._lead_time, ds_config.frequency.attribute))

                additional_lead_dates = []

                for date in dates:
                    for time in range(self._lead_time):
                        attrs = {"{}s".format(ds_config.frequency.attribute): time + 1}
                        lead_date = date + relativedelta(**attrs)

                        if lead_date not in dates:
                            if all([os.path.exists(ds_config.var_filepath(var_config, [lead_date]))
                                    for var_config in ds_config.variables]):
                                # We only add these dates into the mix if all necessary files exist
                                additional_lead_dates.append(lead_date)
                            else:
                                # Otherwise, warn that the lag data means this is being dropped
                                logging.warning("{} will be dropped from {} due to missing lead data {}".
                                                format(date, split, lead_date))
                                drop_dates[split].append(date)
                dates += list(set(additional_lead_dates))

        for split in self._splits.keys():
            dates = sorted([data_date for data_date in self._splits[split] if data_date not in drop_dates[split]])
            self._source_files[split] = {var_config.name: sorted(ds_config.var_filepaths(var_config, dates))
                                         for var_config in ds_config.variables}

            for var_name, var_files in self._source_files[split].items():
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
                self.dtype(el)
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
                self.dtype(el)
                for el in open(scale_path, "r").read().split(",")
            ])
        elif self.norm_split_dates:
            logging.debug("Generating norm-scaling min-max from {} training "
                          "dates".format(len(self.norm_split_dates)))

            norm_samples = da.sel(time=self.norm_split_dates).data
            norm_samples = norm_samples.ravel()

            minimum = dask.array.nanmin(norm_samples).astype(self.dtype)
            maximum = dask.array.nanmax(norm_samples).astype(self.dtype)
        else:
            raise RuntimeError("Either a normalisation file or training data "
                               "must be supplied")

        new_da = (da - minimum) / (maximum - minimum)
        if not self._refdir:
            open(scale_path, "w").write(",".join([str(f) for f in [float(minimum), float(maximum)]]))
        return new_da

    def _process_channel(self,
                         var_name: str,
                         var_suffix: str):
        """

        :param var_name:
        :param var_suffix:
        """

        with dask.config.set(**{'array.slicing.split_large_chunks': True}):
            source_files = list(sorted(set([file
                                            for split, var_files in self.source_files.items()
                                            for vn, files in var_files.items()
                                            for file in files
                                            if var_name == vn])))

            if len(source_files) > 0:
                logging.info("Opening files for {}".format(var_name))
                logging.debug("Files: {}".format(source_files))

                # In the old IceNet library there was dubiousness about the source of the
                # data so this was harder. Now we work with whatever we get from download-toolbox
                ds = xr.open_mfdataset(
                    source_files,
                    # Solves issue with inheriting files without
                    # time dimension (only having coordinate)
                    combine="nested",
                    concat_dim="time",
                    coords="minimal",
                    compat="override",
                    # TODO: review this, but if lat-lon is in the file, it's signalling bigger issues
                    # drop_variables=("lat", "lon"),
                    parallel=self._parallel)
                da = getattr(ds, var_name)
                da = da.astype(self.dtype)

                # FIXME: we should ideally store train dates against the
                #  normalisation and climatology, to ensure recalculation on
                #  reprocess. All this need be is in the path, to be honest

                if var_suffix == "anom":
                    if len(self._anom_clim_splits) < 1:
                        raise ProcessingError("You must provide a list of splits via "
                                              "anom_clim_splits if you have anomoly channels")

                    if self._refdir:
                        logging.info("Loading climatology from alternate directory: {}".format(self._refdir))
                        clim_path = os.path.join(self._refdir, "params", "climatology.{}".format(var_name))
                    else:
                        clim_path = os.path.join(self.get_data_var_folder("params"), "climatology.{}".format(var_name))

                    # TODO: farm out with adaptive frequency the generation of climatologies
                    if not os.path.exists(clim_path):
                        logging.info("Generating climatology {}".format(clim_path))

                        if len(self.anom_split_dates) > 0:
                            climatology = da.sel(time=self.anom_split_dates).\
                                groupby('time.month', restore_coord_dims=True).\
                                mean()

                            climatology.to_netcdf(clim_path)
                        else:
                            raise ProcessingError(
                                "{} does not exist and no dates are supplied valid for generation".
                                format(clim_path))
                    else:
                        logging.info("Reusing climatology {}".format(clim_path))
                        climatology = xr.open_dataarray(clim_path)

                    if not set(da.groupby("time.month").all().month.values).\
                            issubset(set(climatology.month.values)):
                        logging.warning(
                            "We don't have a full climatology ({}) "
                            "compared with data ({})".format(
                                ",".join(
                                    [str(i) for i in climatology.month.values]),
                                ",".join([
                                    str(i) for i in da.groupby(
                                        "time.month").all().month.values
                                ])))
                        da = da - climatology.mean()
                    else:
                        da = da.groupby("time.month") - climatology

                da = self.pre_normalisation(var_name, da)
                # We don't do this (https://github.com/tom-andersson/icenet2/
                # blob/4ca0f1300fbd82335d8bb000c85b1e71855630fa/icenet2/utils.py#L520) any more

                if self._linear_trends is not None:
                    if var_name in self._linear_trends and var_suffix == "abs":
                        # TODO: verify, this used to be da = , but we should not be
                        #  overwriting the abs da with linear trend da
                        ref_da = None

                        if self._refdir:
                            logging.info(
                                "We have a reference {}, so will load "
                                "and supply abs from that for linear trend of "
                                "{}".format(self._refdir, var_name))
                            ref_da = xr.open_dataarray(
                                os.path.join(self._refdir, var_name,
                                             "{}_{}.nc".format(var_name, var_suffix)))

                        self._build_linear_trend_da(da, var_name, ref_da=ref_da)

                    elif var_name in self._linear_trends \
                            and var_name not in self._abs_vars:
                        raise NotImplementedError(
                            "You've asked for linear trend "
                            "without an  absolute value var: {}".format(var_name))

                if var_name in self._no_normalise:
                    logging.info("No normalisation for {}".format(var_name))
                else:
                    logging.info("Normalising {}".format(var_name))
                    da = self._normalise(var_name, da)

                da = self.post_normalisation(var_name, da)

                self.save_processed_file(
                    "{}_{}".format(var_name, var_suffix),
                    "{}_{}.nc".format(var_name, var_suffix),
                    da.rename("_".join([var_name, var_suffix])))

    def get_config(self, **kwargs):
        """

        Args:
            **kwargs:

        """

        return {
            "implementation": "{}:{}".format(self.__module__, self.__class__.__name__),
            "anomoly_vars": self._anom_vars,
            "absolute_vars": self.abs_vars,
            "dataset_config": self._dataset_config,
            "lag_time": self._lag_time,
            "lead_time": self._lead_time,
            "linear_trends": self._linear_trends,
            "linear_trend_steps": self._linear_trend_steps,
            "path": self.path,
            "processed_files": self._processed_files,
            "source_files": self._source_files,
            "splits": self.splits,
        }

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

    def pre_normalisation(self, var_name: str, da: object):
        """

        :param var_name:
        :param da:
        :return:
        """
        logging.debug(
            "No pre normalisation implemented for {}".format(var_name))
        return da

    def post_normalisation(self, var_name: str, da: object):
        """

        :param var_name:
        :param da:
        :return:
        """
        logging.debug(
            "No post normalisation implemented for {}".format(var_name))
        return da

    def process(self):
        var_suffixes = ["abs", "anom"]
        var_lists = [getattr(self, "_{}_vars".format(vs)) for vs in var_suffixes]
        for var_suffix, var_list in zip(var_suffixes, var_lists):
            for var_name in var_list:
                if var_name not in set([source_name
                                        for split, split_vars in self.source_files.items()
                                        for source_name in split_vars.keys()]):
                    logging.warning("{} does not exist in data, you can't use it as a variable".format(var_name))
                else:
                    self._process_channel(var_name, var_suffix)

        self.save_config()

    @property
    def anom_split_dates(self) -> list:
        # TODO: functools.cached_property, though slightly odd behaviour re. write-ability
        return [date
                for clim_split in self._anom_clim_splits
                for date in self._splits[clim_split]]

    @property
    def dataset_config(self):
        return self._dataset_config

    @property
    def lag_time(self) -> int:
        """The lead time used in the data processing."""
        return self._lag_time

    @property
    def lead_time(self) -> int:
        """The lead time used in the data processing."""
        return self._lead_time

    @property
    def norm_split_dates(self):
        # TODO: functools.cached_property, though slightly odd behaviour re. write-ability
        return [date
                for clim_split in self._normalisation_splits
                for date in self._splits[clim_split]]

    @property
    def source_files(self) -> dict:
        return self._source_files

    @property
    def splits(self) -> object:
        """The dates used for training, validation, and testing in this class as a named collections.tuple."""
        return self._splits

