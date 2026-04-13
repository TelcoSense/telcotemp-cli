from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyproj import Transformer

from telcotemp.processing.ml_modeling import (
    get_ml_prediction_lookback,
    temperature_predict,
)


@dataclass
class BiasReportResult:
    df: pd.DataFrame
    csv_path: str
    summary_csv_path: str
    plot_bias_vs_sun_path: str
    plot_bias_vs_hour_path: str
    mae: float
    mean_bias: float
    sample_count: int


def _empty_bias_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "t10",
            "ID",
            "cml_med",
            "meteo_med",
            "sun",
            "Hour",
            "cml_count",
            "nearest_distance_m",
            "bias",
            "abs_error",
        ]
    )


def _to_10min_median(
    df: pd.DataFrame, time_col: str, value_col: str, out_col: str
) -> pd.DataFrame:
    d = df[[time_col, value_col]].copy()
    d = d.dropna(subset=[time_col])
    d["t10"] = pd.to_datetime(d[time_col]).dt.floor("10min")
    g = d.groupby("t10", as_index=False)[value_col].median()
    g = g.rename(columns={value_col: out_col})
    return g


def _with_match_xy(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if {"Longitude", "Latitude"}.issubset(out.columns):
        lon = pd.to_numeric(out["Longitude"], errors="coerce")
        lat = pd.to_numeric(out["Latitude"], errors="coerce")
        valid = lon.notna() & lat.notna()
        out["_match_x"] = np.nan
        out["_match_y"] = np.nan
        if valid.any():
            transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
            xs, ys = transformer.transform(lon.loc[valid].to_numpy(), lat.loc[valid].to_numpy())
            out.loc[valid, "_match_x"] = xs
            out.loc[valid, "_match_y"] = ys
        return out

    if {"X", "Y"}.issubset(out.columns):
        out["_match_x"] = pd.to_numeric(out["X"], errors="coerce")
        out["_match_y"] = pd.to_numeric(out["Y"], errors="coerce")
        return out

    out["_match_x"] = np.nan
    out["_match_y"] = np.nan
    return out


def _prepare_cml_station_matches(
    cml_df: pd.DataFrame,
    meteo_df: pd.DataFrame,
    *,
    cml_temp_col: str,
    meteo_temp_col: str,
    max_distance_m: float,
) -> pd.DataFrame:
    required_cml = {"Time", cml_temp_col, "sun", "Hour"}
    required_meteo = {"Time", meteo_temp_col, "ID"}

    if not required_cml.issubset(cml_df.columns) or not required_meteo.issubset(
        meteo_df.columns
    ):
        return _empty_bias_df()

    cml_key = "IP" if "IP" in cml_df.columns else "Link_ID"
    if cml_key not in cml_df.columns:
        return _empty_bias_df()

    cml_df = _with_match_xy(cml_df)
    meteo_df = _with_match_xy(meteo_df)

    cml = cml_df[
        ["Time", cml_key, cml_temp_col, "_match_x", "_match_y", "sun", "Hour"]
    ].copy()
    cml = cml.dropna(subset=["Time", cml_key, cml_temp_col, "_match_x", "_match_y"])
    cml["t10"] = pd.to_datetime(cml["Time"]).dt.floor("10min")
    cml = (
        cml.groupby(["t10", cml_key], as_index=False)
        .agg(
            cml_med=(cml_temp_col, "median"),
            match_x=("_match_x", "median"),
            match_y=("_match_y", "median"),
            sun=("sun", "median"),
            Hour=("Hour", "first"),
        )
        .rename(columns={cml_key: "cml_id"})
    )

    met = meteo_df[["Time", "ID", meteo_temp_col, "_match_x", "_match_y"]].copy()
    met = met.dropna(subset=["Time", "ID", meteo_temp_col, "_match_x", "_match_y"])
    met["t10"] = pd.to_datetime(met["Time"]).dt.floor("10min")
    met = met.groupby(["t10", "ID"], as_index=False).agg(
        meteo_med=(meteo_temp_col, "median"),
        match_x=("_match_x", "median"),
        match_y=("_match_y", "median"),
    )

    if cml.empty or met.empty:
        return _empty_bias_df()

    cml_points = cml[["cml_id", "match_x", "match_y"]].drop_duplicates("cml_id").copy()
    stations = met[["ID", "match_x", "match_y"]].drop_duplicates("ID").copy()

    if cml_points.empty or stations.empty:
        return _empty_bias_df()

    cml_xy = cml_points[["match_x", "match_y"]].to_numpy(dtype=float)
    pair_frames = []

    for station in stations.itertuples(index=False):
        dist = np.sqrt(
            (cml_xy[:, 0] - float(station.match_x)) ** 2
            + (cml_xy[:, 1] - float(station.match_y)) ** 2
        )
        keep = dist <= max_distance_m
        if not np.any(keep):
            continue
        pair_frames.append(
            pd.DataFrame(
                {
                    "ID": station.ID,
                    "cml_id": cml_points.loc[keep, "cml_id"].to_numpy(),
                    "distance_m": dist[keep],
                }
            )
        )

    if not pair_frames:
        return _empty_bias_df()

    pairs = pd.concat(pair_frames, ignore_index=True)

    matched = met.merge(pairs, on="ID", how="inner").merge(
        cml[["t10", "cml_id", "cml_med", "sun", "Hour"]],
        on=["t10", "cml_id"],
        how="inner",
    )

    if matched.empty:
        return _empty_bias_df()

    out = matched.groupby(["t10", "ID"], as_index=False).agg(
        cml_med=("cml_med", "median"),
        meteo_med=("meteo_med", "first"),
        sun=("sun", "median"),
        Hour=("Hour", "first"),
        cml_count=("cml_id", "nunique"),
        nearest_distance_m=("distance_m", "min"),
    )
    out["bias"] = out["cml_med"] - out["meteo_med"]
    out["abs_error"] = out["bias"].abs()
    return out


def build_bias_timeseries(
    cml_df: pd.DataFrame,
    meteo_df: pd.DataFrame,
    cml_temp_col: str = "Predicted_Temperature",
    meteo_temp_col: str = "Temperature_Value",
    *,
    spatial_match_enabled: bool = True,
    spatial_match_radius_m: float = 10000.0,
) -> pd.DataFrame:
    if spatial_match_enabled:
        return _prepare_cml_station_matches(
            cml_df,
            meteo_df,
            cml_temp_col=cml_temp_col,
            meteo_temp_col=meteo_temp_col,
            max_distance_m=float(spatial_match_radius_m),
        )

    # 10-min medians
    cml_med = _to_10min_median(cml_df, "Time", cml_temp_col, "cml_med")
    met_med = _to_10min_median(meteo_df, "Time", meteo_temp_col, "meteo_med")

    out = cml_med.merge(met_med, on="t10", how="inner")
    out["bias"] = out["cml_med"] - out["meteo_med"]
    out["abs_error"] = out["bias"].abs()

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
    if not {"bias", "sun"}.issubset(df_bias.columns):
        d = _empty_bias_df()
    else:
        d = df_bias
    d = d.dropna(subset=["bias", "sun"]).copy()
    if d.empty:
        plt.figure()
        plt.text(0.5, 0.5, "No matched samples", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(out_path)
        plt.close()
        return
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
    if not {"bias", "Hour"}.issubset(df_bias.columns):
        d = _empty_bias_df()
    else:
        d = df_bias
    d = d.dropna(subset=["bias", "Hour"]).copy()
    if d.empty:
        plt.figure()
        plt.text(0.5, 0.5, "No matched samples", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(out_path)
        plt.close()
        return
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
    spatial_match_enabled: bool = True,
    spatial_match_radius_m: float = 10000.0,
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

    df_bias = build_bias_timeseries(
        cml_df,
        met_df,
        spatial_match_enabled=spatial_match_enabled,
        spatial_match_radius_m=spatial_match_radius_m,
    )

    stamp = f"{pd.to_datetime(start_time).strftime('%Y%m%d_%H%M')}-{pd.to_datetime(end_time).strftime('%Y%m%d_%H%M')}"
    csv_path = os.path.join(out_dir, f"bias_{stamp}.csv")
    summary_csv_path = os.path.join(out_dir, f"bias_summary_{stamp}.csv")
    p1 = os.path.join(out_dir, f"bias_vs_sun_{stamp}.png")
    p2 = os.path.join(out_dir, f"bias_vs_hour_{stamp}.png")

    sample_count = int(len(df_bias))
    mae = (
        float(df_bias["abs_error"].mean())
        if sample_count and "abs_error" in df_bias.columns
        else float("nan")
    )
    mean_bias = (
        float(df_bias["bias"].mean()) if sample_count and "bias" in df_bias.columns else float("nan")
    )
    summary_df = pd.DataFrame(
        [
            {
                "start_time": pd.to_datetime(start_time),
                "end_time": pd.to_datetime(end_time),
                "sample_count": sample_count,
                "mae": mae,
                "mean_bias": mean_bias,
                "comparison_mode": (
                    "station_radius_median"
                    if spatial_match_enabled
                    else "global_10min_median"
                ),
                "spatial_match_radius_m": float(spatial_match_radius_m),
            }
        ]
    )

    logger = getattr(cml_engine, "logger", logging.getLogger(__name__))
    if spatial_match_enabled:
        logger.info(
            "[DIAGNOSTICS] Spatial station matching enabled: radius=%.0f m, matched_samples=%d",
            float(spatial_match_radius_m),
            sample_count,
        )
        if sample_count == 0:
            logger.warning(
                "[DIAGNOSTICS] No station-CML matches found for %s -> %s within %.0f m",
                pd.to_datetime(start_time).strftime("%Y-%m-%d %H:%M"),
                pd.to_datetime(end_time).strftime("%Y-%m-%d %H:%M"),
                float(spatial_match_radius_m),
            )

    df_bias.to_csv(csv_path, index=False)
    summary_df.to_csv(summary_csv_path, index=False)
    plot_bias_vs_sun(df_bias, p1)
    plot_bias_vs_hour(df_bias, p2)

    return BiasReportResult(
        df=df_bias,
        csv_path=csv_path,
        summary_csv_path=summary_csv_path,
        plot_bias_vs_sun_path=p1,
        plot_bias_vs_hour_path=p2,
        mae=mae,
        mean_bias=mean_bias,
        sample_count=sample_count,
    )
