import logging
import os

from pprint import pformat

import dask
import dask.array
import numpy as np
import orjson
import pandas as pd
import xarray as xr

from preprocess_toolbox.base import Processor, ProcessingError
from preprocess_toolbox.models import linear_trend_forecast

from download_toolbox.interface import Frequency


class NormalisingChannelProcessor(Processor):
    """

    TODO: at the moment this class implementation is solely intended to break apart
     the heavier lifting happening via xarray, for the most part

    """

    def __init__(self,
                 *args,
                 clim_frequency: Frequency = Frequency.MONTH,
                 parallel_opens: bool = True,
                 **kwargs):
        super().__init__(*args, **kwargs)

        if clim_frequency != Frequency.MONTH:
            raise NotImplementedError("We only generate climatologies at a monthly resolution, "
                                      "this needs implementation")

        self._parallel = parallel_opens

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
        data_dates = sorted(
            [pd.Timestamp(date) for date in input_da.time.values])

        trend_dates = set()
        trend_steps = max(self._linear_trend_steps)
        logging.info(
            "Generating trend data up to {} steps ahead for {} dates".format(
                trend_steps, len(data_dates)))

        for dat_date in data_dates:
            trend_dates = trend_dates.union([
                dat_date + pd.DateOffset(days=d)
                for d in self._linear_trend_steps
            ])

        trend_dates = list(sorted(trend_dates))
        logging.info("Generating {} trend dates".format(len(trend_dates)))

        linear_trend_da = \
            xr.broadcast(input_da, xr.DataArray(pd.date_range(
                data_dates[0],
                data_dates[-1] + pd.DateOffset(days=trend_steps)),
                    dims="time"))[0]
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
            date_da = da[(da.time['time.month'] == target_date.month) &
                         (da.time['time.day'] == target_date.day) &
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
                    missing_dates=self._missing_dates,
                    shape=ref_da.isel(time=0).shape, # shape
                )

            linear_trend_da.loc[dict(time=forecast_date)] = output_map

        logging.info("Writing new trend cache for {}".format(var_name))
        trend_cache.close()
        linear_trend_da = linear_trend_da.rename("{}_linear_trend".format(var_name))
        self.save_processed_file(var_name,
                                 "{}_linear_trend.nc".format(var_name),
                                 linear_trend_da)

        return linear_trend_da

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
                    drop_variables=("lat", "lon"),
                    parallel=self._parallel)
                da = getattr(ds, var_name)

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

                # FIXME: this is not the way to reconvert underlying data on
                #  dask arrays (da.astype should be used - test with dask)
                da.data = np.asarray(da.data, dtype=self._dtype)

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

                    logging.debug(pformat(da.time.values))
                    logging.debug(pformat(self.norm_split_dates))

                    da = self._normalise(var_name, da)

                da = self.post_normalisation(var_name, da)

                self.save_processed_file(
                    var_name,
                    "{}_{}.nc".format(var_name, var_suffix),
                    da.rename("_".join([var_name, var_suffix])))

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

    def get_config(self):
        """

        :return:
        """

        return {
            "implementation": self.__class__.__name__,
            "anom": self._anom_vars,
            "abs": self._abs_vars,
            "splits": self.splits,
            "linear_trends": self._linear_trends,
            "linear_trend_steps": self._linear_trend_steps,
            "processed_files": self._processed_files,
        }

    def save_processed_file(self,
                            var_name: str,
                            name: str,
                            data: object,
                            **kwargs) -> str:
        """Save processed data to netCDF file.


        :param var_name: The name of the variable.
        :param name: The name of the file.
        :param data: The data to be saved.

        :return: The path of the saved netCDF file.
        """
        file_path = os.path.join(self.path, name)
        data.to_netcdf(file_path)

        if var_name not in self.processed_files.keys():
            self.processed_files[var_name] = list()

        if file_path not in self.processed_files[var_name]:
            logging.debug("Adding {} file: {}".format(var_name, file_path))
            self.processed_files[var_name].append(file_path)
        else:
            logging.warning("{} already exists in {} processed list".format(file_path, var_name))
        return file_path
