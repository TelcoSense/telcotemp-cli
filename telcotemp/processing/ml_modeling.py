from __future__ import annotations

from datetime import timedelta

import pandas as pd


def get_ml_prediction_lookback(ml_cfg: dict | None) -> timedelta:
    if not ml_cfg:
        return timedelta(0)
    from telcotemp.neural.pytorch_sequence import resolve_window_settings

    seq_len, sample_minutes = resolve_window_settings(ml_cfg)
    return timedelta(minutes=(seq_len - 1) * sample_minutes)


def temperature_predict(
    df: pd.DataFrame,
    *,
    ml_config: dict,
    prediction_start=None,
    prediction_end=None,
):
    from telcotemp.neural.pytorch_sequence import predict_temperature_sequence

    return predict_temperature_sequence(
        df,
        ml_cfg=dict(ml_config),
        prediction_start=prediction_start,
        prediction_end=prediction_end,
    )
