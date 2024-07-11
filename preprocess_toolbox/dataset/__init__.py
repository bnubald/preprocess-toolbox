import logging
import os
import shutil

from download_toolbox.interface import get_dataset_config_implementation


def clone_dataset(source_config: os.PathLike,
                  destination_id: str):
    """

    TODO: this can be in download_toolbox.dataset.DatasetConfig

    :param source_config:
    :param destination_id:
    :return:
    """
    logging.debug("Got destination {}, copying dataset".format(destination_id))
    ds_config = get_dataset_config_implementation(source_config)

    base, append = [el.strip(os.sep) for el in ds_config.path.split(ds_config.identifier)]
    destination = os.path.join(base, destination_id, append)

    if not os.path.exists(destination):
        shutil.copytree(ds_config.path, destination)
        ds_config.config.render(ds_config, directory=os.path.join(base, destination_id))
        logging.info("Dataset cloned to configuration: {}".format(ds_config.config_file))
    else:
        raise RuntimeError(
            "{} already exists, please remove or think about destructive processing".format(destination_id))

    # TODO: does this indicate poor composition, in that a DataCollection has a config, but DatasetConfig
    #  also has a config parameter for the underlying config? Bit of a strange
    #  structure, should Configuration should be opaque from the "public interface"
    return ds_config.config_file
