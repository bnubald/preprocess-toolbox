import logging
import os
import re

import iris

from preprocess_toolbox.data.geo import (gridcell_angles_from_dim_coords,
                                         invert_gridcell_angles,
                                         rotate_grid_vectors)


def rotate_data(ref_ds: object,
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
