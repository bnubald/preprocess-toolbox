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
                          start_date: dt.date = None):
    """

    TODO: we need to be able to add more missing dates as detected spatially (full of nans)
    TODO: we should be limiting interpolation or doing the above when over n steps

    Args:
        ds:
        ds_config:
        variable:
        end_date:
        start_date:

    Returns:

    """
    da = getattr(ds, variable)
    da = da.sortby('time')

    dates_obs = [pd.to_datetime(date).date() for date in da.time.values]
    dates_all = [pd.to_datetime(date).date() for date in
                 pd.date_range(min(dates_obs) if not start_date else start_date,
                               max(dates_obs) if not end_date else end_date,
                               freq="1{}".format(ds_config.frequency.freq))]

    invalid_dates = list() if not hasattr(ds_config, "invalid_dates") else ds_config.invalid_dates
    drop_dates = [pd.Timestamp(el) for el in invalid_dates if pd.Timestamp(el) in da.time.values]
    da = da.drop_sel(time=drop_dates)
    missing_dates = [date for date in dates_all
                     if date not in dates_obs or date in invalid_dates]

    logging.info("Interpolating {} missing dates".format(len(missing_dates)))

    for date in missing_dates:
        if pd.Timestamp(date) not in da.time.values:
            logging.info("Interpolating {}".format(date))
            da = xr.concat([da,
                            da.interp(time=pd.to_datetime(date))],
                           dim='time')

    logging.debug("Finished interpolation")

    da = da.sortby('time')
    ds[variable] = da
    return ds
