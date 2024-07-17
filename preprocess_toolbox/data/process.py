import logging
import os
import re

import iris

from preprocess_toolbox.data.spatial import (gridcell_angles_from_dim_coords,
                                             invert_gridcell_angles,
                                             rotate_grid_vectors)


def regrid_dataset(ref_ds: object,
                   process_ds: object):
    results = list()

    logging.info("Regridding {} against {}".format(process_ds, ref_ds))
    import sys
    sys.exit(0)

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


def rotate_dataset(ref_ds: object,
                   process_ds: object,
                   vars_to_rotate: object = ("uas", "vas")):
    """

    :param ref_file:
    :param process_files:
    :param vars_to_rotate:
    """
    assert len(vars_to_rotate) == 2, "Too many wind variables supplied: {}, " \
                                     "there should only be two.". \
        format(", ".join(vars_to_rotate))

    ref_cube = get_reference_cube(ref_ds)
    angles = gridcell_angles_from_dim_coords(ref_cube)
    invert_gridcell_angles(angles)

    logging.info("Rotating wind data in {} files based on angles in ")

    wind_files = {}

    for var in vars_to_rotate:
        file_source = process_ds.files
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
    assert len(wind_files[vars_to_rotate[0]]) == len(wind_files[vars_to_rotate[1]]), \
        "The wind file datasets are unequal in length"

    # validation
    for idx, wind_file_0 in enumerate(wind_files[vars_to_rotate[0]]):
        wind_file_1 = wind_files[vars_to_rotate[1]][idx]

        wd0 = re.sub(r'^{}_'.format(vars_to_rotate[0]), '',
                     os.path.basename(wind_file_0))

        if not wind_file_1.endswith(wd0):
            logging.error("File array is not valid: {}".format(zip(wind_files)))
            raise RuntimeError("{} is not at the end of {}, something is "
                               "wrong".format(wd0, wind_file_1))

    for idx, wind_file_0 in enumerate(wind_files[vars_to_rotate[0]]):
        wind_file_1 = wind_files[vars_to_rotate[1]][idx]

        logging.info("Rotating {} and {}".format(wind_file_0, wind_file_1))

        wind_cubes = dict()
        wind_cubes_r = dict()

        wind_cubes[vars_to_rotate[0]] = iris.load_cube(wind_file_0)
        wind_cubes[vars_to_rotate[1]] = iris.load_cube(wind_file_1)

        try:
            wind_cubes_r[vars_to_rotate[0]], wind_cubes_r[vars_to_rotate[1]] = \
                rotate_grid_vectors(
                    wind_cubes[vars_to_rotate[0]],
                    wind_cubes[vars_to_rotate[1]],
                    angles,
                )
        except iris.exceptions.CoordinateNotFoundError:
            logging.exception("Failure to rotate due to coordinate issues. "
                              "moving onto next file")
            continue

        # Original implementation is in danger of lost updates
        # due to potential lazy loading
        for i, name in enumerate([wind_file_0, wind_file_1]):
            # NOTE: implementation with temp file caused problems on NFS
            # mounted filesystem, so avoiding in place of letting iris do it
            temp_name = os.path.join(os.path.split(name)[0],
                                     "temp.{}".format(
                                         os.path.basename(name)))
            logging.debug("Writing {}".format(temp_name))

            iris.save(wind_cubes_r[vars_to_rotate[i]], temp_name)
            os.replace(temp_name, name)
            logging.debug("Overwritten {}".format(name))

    merge_files(new_datafile, moved_datafile, self._drop_vars)
