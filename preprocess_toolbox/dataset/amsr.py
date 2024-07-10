



def missing_dates(self):
    """

    :return:
    """
    filenames = set([os.path.join(
        self.get_data_var_folder("siconca"),
        "{}.nc".format(el.strftime("%Y")))
        for el in self._dates])
    filenames = [f for f in filenames if os.path.exists(f)]

    logging.info("Opening for interpolation: {}".format(filenames))
    ds = xr.open_mfdataset(filenames,
                           combine="nested",
                           concat_dim="time",
                           chunks=dict(time=self._chunk_size, ),
                           parallel=True)
    return self._missing_dates(ds.sea_ice_concentration)

def _missing_dates(self, da: object) -> object:
    """

    :param da:
    :return:
    """
    dates_obs = [pd.to_datetime(date).date() for date in da.time.values]
    dates_all = [pd.to_datetime(date).date() for date in
                 pd.date_range(min(self._dates), max(self._dates))]

    # Weirdly, we were getting future warnings for timestamps, but unsure
    # where from
    missing_dates = [date for date in dates_all
                     if date not in dates_obs]

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








extra_args=[
                            (("-u", "--use-dask"),
                             dict(action="store_true", default=False)),
                            (("-c", "--sic-chunking-size"),
                             dict(type=int, default=10)),
                            (("-dt", "--dask-timeouts"),
                             dict(type=int, default=120)),
                            (("-dp", "--dask-port"),
                             dict(type=int, default=8888))
                         ]

if args.use_dask:
    logging.warning("Attempting to use dask client for SIC processing")
    dw = DaskWrapper(workers=args.workers)
    dw.dask_process(method=sic.download)
else:


