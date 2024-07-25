import datetime as dt
import logging
import os

import numpy as np
import pandas as pd
import xarray as xr

from download_toolbox.dataset import DatasetConfig


def process_missing_dates(ds: xr.Dataset,
                          ds_config: DatasetConfig,
                          variable: str,
                          end_date: dt.date = None,
                          invalid_dates: list[dt.date] = None,
                          missing_dates_path: os.PathLike = "missing_days.csv",
                          start_date: dt.date = None,):
    da = getattr(ds, variable)
    da = da.sortby('time')

    dates_obs = [pd.to_datetime(date).date() for date in da.time.values]
    dates_all = [pd.to_datetime(date).date() for date in
                 pd.date_range(min(dates_obs) if not start_date else start_date,
                               max(dates_obs) if not end_date else end_date,
                               freq="1{}".format(ds_config.frequency.freq))]

    invalid_dates = list() if invalid_dates is None else invalid_dates
    missing_dates = [date for date in dates_all
                     if date not in dates_obs
                     or date in invalid_dates]

    logging.info("Processing {} missing dates".format(len(missing_dates)))

    with open(missing_dates_path, "w") as fh:
        fh.writelines([el.strftime(ds_config.frequency.date_format) for el in missing_dates])

    logging.debug("Interpolating {} missing dates".format(len(missing_dates)))

    for date in missing_dates:
        if pd.Timestamp(date) not in da.time.values:
            logging.info("Interpolating {}".format(date))
            da = xr.concat([da,
                            da.interp(time=pd.to_datetime(date))],
                           dim='time')

    logging.debug("Finished interpolation")

    da = da.sortby('time')
    da.data = np.array(da.data)

    for date in missing_dates:
        date_str = pd.to_datetime(date).strftime(ds_config.frequency.date_format)
        logging.debug("Outputting interpolated missing date for analysis for {}".format(date_str))

        fpath = os.path.join(
            ds_config.path,
            "_missing_time.interp",
            "{}.nc".format(date_str))

        if not os.path.exists(fpath):
            day_da = da.sel(time=slice(date, date))

            logging.info("Writing missing date file {}".format(fpath))
            day_da.to_netcdf(fpath)

    ds[variable] = da
    return ds
