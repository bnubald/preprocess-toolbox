def missing_dates(ds,
                  variable):
    logging.info("Opening for interpolation: {}".format(filenames))
    ds = xr.open_mfdataset(filenames,
                           combine="nested",
                           concat_dim="time",
                           chunks=dict(time=self._chunk_size, ),
                           parallel=True)

    da = getattr(ds, variable)

    if pd.Timestamp(1979, 1, 2) in da.time.values \
        and dt.date(1979, 1, 1) in self._dates \
        and pd.Timestamp(1979, 1, 1) not in da.time.values:
        da_1979_01_01 = da.sel(
            time=[pd.Timestamp(1979, 1, 2)]).copy().assign_coords(
            {'time': [pd.Timestamp(1979, 1, 1)]})
        da = xr.concat([da, da_1979_01_01], dim='time')
        da = da.sortby('time')

    dates_obs = [pd.to_datetime(date).date() for date in da.time.values]
    dates_all = [pd.to_datetime(date).date() for date in
                 pd.date_range(min(self._dates), max(self._dates))]

    # Weirdly, we were getting future warnings for timestamps, but unsure
    # where from
    invalid_dates = [pd.to_datetime(d).date() for d in self._invalid_dates]
    missing_dates = [date for date in dates_all
                     if date not in dates_obs
                     or date in invalid_dates]

    logging.info("Processing {} missing dates".format(len(missing_dates)))

    missing_dates_path = os.path.join(
        self.get_data_var_folder("siconca"), "missing_days.csv")

    with open(missing_dates_path, "a") as fh:
        for date in missing_dates:
            # FIXME: slightly unusual format for Ymd dates
            fh.write(date.strftime("%Y,%m,%d\n"))

    logging.debug("Interpolating {} missing dates".
                  format(len(missing_dates)))

    for date in missing_dates:
        if pd.Timestamp(date) not in da.time.values:
            logging.info("Interpolating {}".format(date))
            da = xr.concat([da,
                            da.interp(time=pd.to_datetime(date))],
                           dim='time')

    logging.debug("Finished interpolation")

    da = da.sortby('time')
    da.data = np.array(da.data, dtype=self._dtype)

    for date in missing_dates:
        date_str = pd.to_datetime(date).strftime("%Y_%m_%d")
        fpath = os.path.join(
            self.get_data_var_folder(
                "siconca", append=[str(pd.to_datetime(date).year)]),
            "missing.{}.nc".format(date_str))

        if not os.path.exists(fpath):
            day_da = da.sel(time=slice(date, date))

            logging.info("Writing missing date file {}".format(fpath))
            day_da.to_netcdf(fpath)

    return da
