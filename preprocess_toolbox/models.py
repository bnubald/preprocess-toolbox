import numpy as np


def linear_trend_forecast(
    usable_selector: callable,
    forecast_date: object,
    da: object,
    mask: object,
    missing_dates: object = (),
    shape: tuple = (432, 432)
) -> object:
    """

    :param usable_selector:
    :param forecast_date:
    :param da:
    :param mask:
    :param missing_dates:
    :param shape:
    :return:
    """
    usable_data = usable_selector(da, forecast_date, missing_dates)

    if len(usable_data.time) < 1:
        return np.full(shape, np.nan)

    x = np.arange(len(usable_data.time))
    y = usable_data.data.reshape(len(usable_data.time), -1)

    # TODO: We might (and likely do) have dask arrays, which play badly with raw numpy lstsq
    #  but this cheekily gets round importing dask, it's a temporary hack though
    if hasattr(y, "compute"):
        y = y.compute()

    src = np.c_[x, np.ones_like(x)]
    r, _, _, _ = np.linalg.lstsq(src, y, rcond=None)
    output_map = np.matmul(np.array([len(usable_data.time), 1]), r).reshape(*shape)

    if mask is not None:
        output_map[mask] = 0.
    output_map[output_map < 0] = 0.
    output_map[output_map > 1] = 1.

    return output_map
