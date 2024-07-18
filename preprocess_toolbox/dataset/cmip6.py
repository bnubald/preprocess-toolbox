def additional_regrid_processing(self,
                                 datafile: str,
                                 cube_ease: object):
    """

    :param datafile:
    :param cube_ease:
    """
    (datafile_path, datafile_name) = os.path.split(datafile)
    var_name = datafile_path.split(os.sep)[self._var_name_idx]

    # TODO: regrid fixes need better implementations
    if var_name == "siconca":
        if self._source == 'MRI-ESM2-0':
            cube_ease.data = cube_ease.data / 100.
        cube_ease.data = cube_ease.data.data
    elif var_name in ["tos", "hus1000"]:
        cube_ease.data = cube_ease.data.data

    if cube_ease.data.dtype != np.float32:
        logging.info("Regrid processing, data type not float: {}".
                     format(cube_ease.data.dtype))
        cube_ease.data = cube_ease.data.astype(np.float32)


