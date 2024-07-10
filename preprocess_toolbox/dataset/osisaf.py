


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


