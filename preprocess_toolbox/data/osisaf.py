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
    return self._missing_dates(ds.ice_conc)


def _missing_dates(self, da: object) -> object:
    """

    :param da:
    :return:
    """
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


def _get_missing_coordinates(self, var, hs, coord):
    """

    :param var:
    :param hs:
    :param coord:
    """
    missing_coord_file = os.path.join(
        self.get_data_var_folder(var), "missing_coord_data.nc")

    if not os.path.exists(missing_coord_file):
        ftp_source_path = self._ftp_osi450.format(2000, 1)

        retrieve_cmd_template_osi450 = \
            "wget -m -nH -nd -O {} " \
            "ftp://osisaf.met.no{}/{}"
        filename_osi450 = \
            "ice_conc_{}_ease2-250_cdr-v2p0_200001011200.nc".format(hs)

        run_command(retrieve_cmd_template_osi450.format(
            missing_coord_file, ftp_source_path, filename_osi450))
    else:
        logging.info("Coordinate path {} already exists".
                     format(missing_coord_file))

    ds = xr.open_dataset(missing_coord_file,
                         drop_variables=var_remove_list,
                         engine="netcdf4").load()
    try:
        coord_data = getattr(ds, coord)
    except AttributeError as e:
        logging.exception("{} does not exist in coord reference file {}".
                          format(coord, missing_coord_file))
        raise RuntimeError(e)
    return coord_data


