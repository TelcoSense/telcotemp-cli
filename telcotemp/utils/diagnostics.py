from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from telcotemp.processing.ml_modeling import (
    get_ml_prediction_lookback,
    temperature_predict,
)


@dataclass
class BiasReportResult:
    df: pd.DataFrame
    csv_path: str
    plot_bias_vs_sun_path: str
    plot_bias_vs_hour_path: str


def _to_10min_median(
    df: pd.DataFrame, time_col: str, value_col: str, out_col: str
) -> pd.DataFrame:
    d = df[[time_col, value_col]].copy()
    d = d.dropna(subset=[time_col])
    d["t10"] = pd.to_datetime(d[time_col]).dt.floor("10min")
    g = d.groupby("t10", as_index=False)[value_col].median()
    g = g.rename(columns={value_col: out_col})
    return g


def build_bias_timeseries(
    cml_df: pd.DataFrame,
    meteo_df: pd.DataFrame,
    cml_temp_col: str = "Predicted_Temperature",
    meteo_temp_col: str = "Temperature_Value",
) -> pd.DataFrame:
    # 10-min medians
    cml_med = _to_10min_median(cml_df, "Time", cml_temp_col, "cml_med")
    met_med = _to_10min_median(meteo_df, "Time", meteo_temp_col, "meteo_med")

    out = cml_med.merge(met_med, on="t10", how="inner")
    out["bias"] = out["cml_med"] - out["meteo_med"]

    # attach sun/hour from CML (median sun, first hour)
    feat = cml_df[["Time", "sun", "Hour"]].copy()
    feat["t10"] = pd.to_datetime(feat["Time"]).dt.floor("10min")
    feat_g = feat.groupby("t10", as_index=False).agg(
        sun=("sun", "median"),
        Hour=("Hour", "first"),
    )

    out = out.merge(feat_g, on="t10", how="left")
    return out


def plot_bias_vs_sun(df_bias: pd.DataFrame, out_path: str) -> None:
    d = df_bias.dropna(subset=["bias", "sun"]).copy()
    # sun is 0/1 -> boxplot
    groups = [
        d.loc[d["sun"] < 0.5, "bias"].to_numpy(),
        d.loc[d["sun"] >= 0.5, "bias"].to_numpy(),
    ]

    plt.figure()
    # plt.boxplot(groups, labels=["night (sun=0)", "day (sun=1)"])
    # plt.ylabel("bias [°C]  (CML median - meteo median)")
    # plt.title("Bias vs daylight")
    plt.boxplot(groups, labels=["noc (sun=0)", "den (sun=1)"])
    plt.ylabel("bias [°C]  (CML median – meteo median)")
    plt.title("Bias vs. noc/den")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_bias_vs_hour(df_bias: pd.DataFrame, out_path: str) -> None:
    d = df_bias.dropna(subset=["bias", "Hour"]).copy()
    # boxplot by hour 0..23 (may be sparse)
    hours = list(range(24))
    data = [d.loc[d["Hour"] == h, "bias"].to_numpy() for h in hours]

    plt.figure()
    plt.boxplot(data, labels=[str(h) for h in hours], showfliers=False)
    # plt.xlabel("Hour")
    # plt.ylabel("bias [°C]  (CML median - meteo median)")
    # plt.title("Bias vs hour")
    plt.xlabel("Hodina dne")
    plt.ylabel("bias [°C]  (CML median – meteo median)")
    plt.title("Bias vs. hodiny")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def run_bias_report(
    *,
    cml_engine,
    meteo_engine,
    start_time,
    end_time,
    out_dir: str,
) -> BiasReportResult:
    """
    cml_engine: CalculationEngine(mode='cml')
    meteo_engine: CalculationEngine(mode='meteo')
    """

    Path(out_dir).mkdir(parents=True, exist_ok=True)

    # Load raw data
    ml_cfg = cml_engine.config.get_ml()
    history_start = start_time - get_ml_prediction_lookback(ml_cfg)

    cml_df = cml_engine.data_source.fetch_data(history_start, end_time)
    cml_df = cml_engine.data_source.prepare_data(cml_df, filter_ids=None)

    cml_df = temperature_predict(
        cml_df,
        ml_config=ml_cfg,
        prediction_start=start_time,
        prediction_end=end_time,
    )

    met_df = meteo_engine.data_source.fetch_data(start_time, end_time)
    met_df = meteo_engine.data_source.prepare_data(met_df, filter_ids=None)

    df_bias = build_bias_timeseries(cml_df, met_df)

    stamp = f"{pd.to_datetime(start_time).strftime('%Y%m%d_%H%M')}-{pd.to_datetime(end_time).strftime('%Y%m%d_%H%M')}"
    csv_path = os.path.join(out_dir, f"bias_{stamp}.csv")
    p1 = os.path.join(out_dir, f"bias_vs_sun_{stamp}.png")
    p2 = os.path.join(out_dir, f"bias_vs_hour_{stamp}.png")

    df_bias.to_csv(csv_path, index=False)
    plot_bias_vs_sun(df_bias, p1)
    plot_bias_vs_hour(df_bias, p2)

    return BiasReportResult(
        df=df_bias,
        csv_path=csv_path,
        plot_bias_vs_sun_path=p1,
        plot_bias_vs_hour_path=p2,
    )
