# From climatedownloader
import iris
import iris.analysis
import iris.cube
import iris.exceptions


# TODO: move        self._rotatable_files = []
# TODO: move        self._sic_ease_cubes = dict()

def rotate_wind_data(self,
                     apply_to: object = ("uas", "vas"),
                     manual_files: object = None):
    """

    :param apply_to:
    :param manual_files:
    """
    assert len(apply_to) == 2, "Too many wind variables supplied: {}, " \
                               "there should only be two.". \
        format(", ".join(apply_to))

    angles = gridcell_angles_from_dim_coords(self.sic_ease_cube)
    invert_gridcell_angles(angles)

    logging.info("Rotating wind data in {}".format(
        " ".join([self.get_data_var_folder(v) for v in apply_to])))

    wind_files = {}

    for var in apply_to:
        source = self.get_data_var_folder(var)

        file_source = self._files_downloaded \
            if not manual_files else manual_files

        latlon_files = [df for df in file_source if source in df]
        wind_files[var] = sorted([
            re.sub(r'{}'.format(self.pregrid_prefix), '', df)
            for df in latlon_files
            if os.path.dirname(df).split(os.sep)
               [self._var_name_idx] == var],
            key=lambda x: int(re.search(r'^(?:\w+_)?(\d+).nc',
                                        os.path.basename(x)).group(1))
        )
        logging.info("{} files for {}".format(len(wind_files[var]), var))

    # NOTE: we're relying on apply_to having equal datasets
    assert len(wind_files[apply_to[0]]) == len(wind_files[apply_to[1]]), \
        "The wind file datasets are unequal in length"

    # validation
    for idx, wind_file_0 in enumerate(wind_files[apply_to[0]]):
        wind_file_1 = wind_files[apply_to[1]][idx]

        wd0 = re.sub(r'^{}_'.format(apply_to[0]), '',
                     os.path.basename(wind_file_0))

        if not wind_file_1.endswith(wd0):
            logging.error("Wind file array is not valid: {}".format(
                zip(wind_files)))
            raise RuntimeError("{} is not at the end of {}, something is "
                               "wrong".format(wd0, wind_file_1))

    for idx, wind_file_0 in enumerate(wind_files[apply_to[0]]):
        wind_file_1 = wind_files[apply_to[1]][idx]

        logging.info("Rotating {} and {}".format(wind_file_0, wind_file_1))

        wind_cubes = dict()
        wind_cubes_r = dict()

        wind_cubes[apply_to[0]] = iris.load_cube(wind_file_0)
        wind_cubes[apply_to[1]] = iris.load_cube(wind_file_1)

        try:
            wind_cubes_r[apply_to[0]], wind_cubes_r[apply_to[1]] = \
                rotate_grid_vectors(
                    wind_cubes[apply_to[0]],
                    wind_cubes[apply_to[1]],
                    angles,
                )
        except iris.exceptions.CoordinateNotFoundError:
            logging.exception("Failure to rotate due to coordinate issues. "
                              "moving onto next file")
            continue

        # Original implementation is in danger of lost updates
        # due to potential lazy loading
        for i, name in enumerate([wind_file_0, wind_file_1]):
            # NOTE: implementation with tempfile caused problems on NFS
            # mounted filesystem, so avoiding in place of letting iris do it
            temp_name = os.path.join(os.path.split(name)[0],
                                     "temp.{}".format(
                                         os.path.basename(name)))
            logging.debug("Writing {}".format(temp_name))

            iris.save(wind_cubes_r[apply_to[i]], temp_name)
            os.replace(temp_name, name)
            logging.debug("Overwritten {}".format(name))



@property
def sic_ease_cube(self):
    """

    :return sic_cube:
    """
    if self._hemisphere not in self._sic_ease_cubes:
        sic_day_fname = 'ice_conc_{}_ease2-250_cdr-v2p0_197901021200.nc'. \
            format(SIC_HEMI_STR[self.hemisphere_str[0]])
        sic_day_path = os.path.join(self.get_data_var_folder("siconca"),
                                    sic_day_fname)
        if not os.path.exists(sic_day_path):
            logging.info("Downloading single daily SIC netCDF file for "
                         "regridding ERA5 data to EASE grid...")

            retrieve_sic_day_cmd = 'wget -m -nH --cut-dirs=6 -P {} ' \
                                   'ftp://osisaf.met.no/reprocessed/ice/' \
                                   'conc/v2p0/1979/01/{}'. \
                format(self.get_data_var_folder("siconca"), sic_day_fname)

            run_command(retrieve_sic_day_cmd)

        # Load a single SIC map to obtain the EASE grid for
        # regridding ERA data
        self._sic_ease_cubes[self._hemisphere] = \
            iris.load_cube(sic_day_path, 'sea_ice_area_fraction')

        # Convert EASE coord units to metres for regridding
        self._sic_ease_cubes[self._hemisphere].coord(
            'projection_x_coordinate').convert_units('meters')
        self._sic_ease_cubes[self._hemisphere].coord(
            'projection_y_coordinate').convert_units('meters')
    return self._sic_ease_cubes[self._hemisphere]


class RegridDownloader(BatchDownloader):
    def regrid(self,
               files: object = None,
               rotate_wind: bool = True):
        """

        :param files:
        """
        filelist = self._files_downloaded if not files else files
        batches = [filelist[b:b + 1000] for b in range(0, len(filelist), 1000)]

        max_workers = min(len(batches), self._max_threads)
        regrid_results = list()

        if max_workers > 0:
            with ThreadPoolExecutor(max_workers=max_workers) \
                    as executor:
                futures = []

                for files in batches:
                    future = executor.submit(self._batch_regrid, files)
                    futures.append(future)

                for future in concurrent.futures.as_completed(futures):
                    try:
                        fut_results = future.result()

                        for res in fut_results:
                            logging.debug("Future result -> regrid_results: {}".
                                          format(res))
                            regrid_results.append(res)
                    except Exception as e:
                        logging.exception("Thread failure: {}".format(e))
        else:
            logging.info("No regrid batches to processing, moving on...")

        if rotate_wind:
            logging.info("Rotating wind data prior to merging")
            self.rotate_wind_data()

        for new_datafile, moved_datafile in regrid_results:
            merge_files(new_datafile, moved_datafile, self._drop_vars)

    def _batch_regrid(self,
                      files: object):
        """

        :param files:
        """
        results = list()

        for datafile in files:
            (datafile_path, datafile_name) = os.path.split(datafile)

            new_filename = re.sub(r'^{}'.format(
                self.pregrid_prefix), '', datafile_name)
            new_datafile = os.path.join(datafile_path, new_filename)

            moved_datafile = None

            if os.path.exists(new_datafile):
                moved_filename = "moved.{}".format(new_filename)
                moved_datafile = os.path.join(datafile_path, moved_filename)
                os.rename(new_datafile, moved_datafile)

                logging.info("{} already existed, moved to {}".
                             format(new_filename, moved_filename))

            logging.debug("Regridding {}".format(datafile))

            try:
                cube = iris.load_cube(datafile)
                cube = self.convert_cube(cube)

                cube_ease = cube.regrid(
                    self.sic_ease_cube, iris.analysis.Linear())

            except iris.exceptions.CoordinateNotFoundError:
                logging.warning("{} has no coordinates...".
                                format(datafile_name))
                if self.delete:
                    logging.debug("Deleting failed file {}...".
                                  format(datafile_name))
                    os.unlink(datafile)
                continue

            self.additional_regrid_processing(datafile, cube_ease)

            logging.info("Saving regridded data to {}... ".format(new_datafile))
            iris.save(cube_ease, new_datafile, fill_value=np.nan)
            results.append((new_datafile, moved_datafile))

            if self.delete:
                logging.info("Removing {}".format(datafile))
                os.remove(datafile)

        return results

    def convert_cube(self, cube: object):
        """Converts Iris cube to be fit for regrid

        :param cube: the cube requiring alteration
        :return cube: the altered cube
        """

        cube = assign_lat_lon_coord_system(cube)
        return cube

    @abstractmethod
    def additional_regrid_processing(self,
                                     datafile: str,
                                     cube_ease: object):
        """

        :param datafile:
        :param cube_ease:
        """
        pass


