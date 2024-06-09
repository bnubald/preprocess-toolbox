def postprocess(self,
                var: str,
                download_path: object):
    """Processing of CDS downloaded files

    If we've not used the toolbox to download the files, we have a lot of
    hourly data to average out, which is taken care of here

    :param var:
    :param download_path:
    """
    # if not self._use_toolbox:
    logging.info("Postprocessing CDS API data at {}".format(download_path))

    temp_path = "{}.bak{}".format(*os.path.splitext(download_path))
    logging.debug("Moving to {}".format(temp_path))
    os.rename(download_path, temp_path)

    ds = xr.open_dataset(temp_path)
    nom = list(ds.data_vars)[0]
    da = getattr(ds.rename({nom: var}), var)

    doy_counts = da.time.groupby("time.dayofyear").count()

    # There are situations where the API will spit out unordered and
    # partial data, so we ensure here means come from full days and don't
    # leave gaps. If we can avoid expver with this, might as well, so
    # that's second
    # FIXME: This will cause issues for already processed latlon data
    if len(doy_counts[doy_counts < 24]) > 0:
        strip_dates_before = min([
            dt.datetime.strptime("{}-{}".format(
                d, pd.to_datetime(da.time.values[0]).year), "%j-%Y")
            for d in doy_counts[doy_counts < 24].dayofyear.values])
        da = da.where(da.time < pd.Timestamp(strip_dates_before), drop=True)

    if 'expver' in da.coords:
        logging.warning("expvers {} in coordinates, will process out but "
                        "this needs further work: expver needs storing for "
                        "later overwriting".format(da.expver))
        # Ref: https://confluence.ecmwf.int/pages/viewpage.action?pageId=173385064
        da = da.sel(expver=1).combine_first(da.sel(expver=5))

    da = da.sortby("time").resample(time='1D').mean()
    da.to_netcdf(download_path)


def additional_regrid_processing(self,
                                 datafile: str,
                                 cube_ease: object):
    """

    :param datafile:
    :param cube_ease:
    """
    (datafile_path, datafile_name) = os.path.split(datafile)
    var_name = datafile_path.split(os.sep)[self._var_name_idx]

    if var_name == 'tos':
        logging.debug("ERA5 regrid postprocess: {}".format(var_name))
        cube_ease.data = cube_ease.data.data
        cube_ease.data = np.where(np.isnan(cube_ease.data), 0., cube_ease.data)
    elif var_name in ['zg500', 'zg250']:
        # Convert from geopotential to geopotential height
        logging.debug("ERA5 additional regrid: {}".format(var_name))
        cube_ease.data /= 9.80665
