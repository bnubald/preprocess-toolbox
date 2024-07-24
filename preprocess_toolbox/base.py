from abc import abstractmethod

import logging
import os

import dask.array
import numpy as np
from dateutil.relativedelta import relativedelta
from pprint import pformat

from download_toolbox.config import Configuration
from download_toolbox.interface import DataCollection, DatasetConfig, Frequency


class ProcessingError(RuntimeError):
    pass


class Processor(DataCollection):
    """An abstract base class for data processing classes.

    Provides methods for initialising source data from download-toolbox defined
    configurations, process the data, and saving the processed data to normalised netCDF files.

    TODO: the majority of actual data processing, for the moment, is being isolated in the
     child implementation of NormalisingChannelProcessor whilst I work out what is going
     on regarding the relevant processing and the inheritance hierarchy that support future,
     diverse implementations for alternative data types not based on xarray

    """

    def __init__(self,
                 dataset_config: DatasetConfig,
                 absolute_vars: list,
                 identifier: str,
                 base_path: os.PathLike = os.path.join(".", "processed"),
                 dtype: np.typecodes = np.float32,
                 update_key: str = None,
                 **kwargs) -> None:
        """

        Args:
            dataset_config:
            absolute_vars:
            identifier:
            base_path:
            dtype:
            update_key:
            **kwargs:
        """
        super().__init__(base_path=base_path,
                         config_type="processed",
                         identifier=identifier,
                         path_components=[])

        self.config.directory = "."

        self._abs_vars = absolute_vars if absolute_vars else []
        self._dtype = dtype

        self._processed_files = dict()

        self._update_key = self.identifier if not update_key else update_key

    def get_data_var_folder(self,
                            var_name: str,
                            append: object = None,
                            missing_error: bool = False) -> os.PathLike:
        """Returns the path for a specific data variable.

        Appends additional folders to the path if specified in the `append` parameter.

        :param var_name: The data variable.
        :param append: Additional folders to append to the path.
        :param missing_error: Flag to specify if missing directories should be treated as an error.

        :return str: The path for the specific data variable.
        """
        if not append:
            append = []

        data_var_path = os.path.join(self.path, *[var_name, *append])

        if not os.path.exists(data_var_path):
            if not missing_error:
                os.makedirs(data_var_path, exist_ok=True)
            else:
                raise OSError("Directory {} is missing and this is "
                              "flagged as an error!".format(data_var_path))

        return data_var_path

    def save_processed_file(self,
                            var_name: str,
                            name: str,
                            data: object) -> str:
        """Save processed data to netCDF file.


        :param var_name: The name of the variable.
        :param name: The name of the file.
        :param data: The data to be saved.

        :return: The path of the saved netCDF file.
        """
        file_path = os.path.join(self.path, name)
        data.to_netcdf(file_path)

        if var_name not in self.processed_files.keys():
            self.processed_files[var_name] = list()

        if file_path not in self.processed_files[var_name]:
            logging.debug("Adding {} file: {}".format(var_name, file_path))
            self.processed_files[var_name].append(file_path)
        else:
            logging.warning("{} already exists in {} processed list".format(file_path, var_name))
        return file_path

    @abstractmethod
    def process(self):
        pass

    @property
    def abs_vars(self):
        return self._abs_vars

    @property
    def dtype(self):
        return self._dtype

    @property
    def processed_files(self) -> dict:
        """A dict with the processed files organised by variable name."""
        return self._processed_files


