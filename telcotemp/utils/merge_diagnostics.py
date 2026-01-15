from __future__ import annotations

import glob
import os
from pathlib import Path

import pandas as pd

from telcotemp.utils.diagnostics import plot_bias_vs_sun, plot_bias_vs_hour


def merge_bias_csvs(bias_dir: str, out_csv: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(bias_dir, "bias_*.csv")))
    if not files:
        raise FileNotFoundError(f"No bias_*.csv found in: {bias_dir}")

    dfs = []
    for f in files:
        df = pd.read_csv(f)
        dfs.append(df)

    out = pd.concat(dfs, ignore_index=True)

    if "t10" in out.columns:
        out["t10"] = pd.to_datetime(out["t10"])
        out = out.sort_values("t10")
        out = out.drop_duplicates(subset=["t10"], keep="last")
    else:
        if "Time" in out.columns:
            out["Time"] = pd.to_datetime(out["Time"])
            out = out.sort_values("Time")
            out = out.drop_duplicates(subset=["Time"], keep="last")

    Path(os.path.dirname(out_csv)).mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)
    return out


def main():
    bias_dir = "outputs_diagnostics"
    out_dir = bias_dir

    merged_csv = os.path.join(out_dir, "bias_merged.csv")
    df = merge_bias_csvs(bias_dir, merged_csv)

    plot_bias_vs_sun(df, os.path.join(out_dir, "bias_vs_sun_merged.pdf"))
    plot_bias_vs_hour(df, os.path.join(out_dir, "bias_vs_hour_merged.pdf"))

    print(f"Merged rows: {len(df)}")
    print(f"Wrote: {merged_csv}")
    print(f"Wrote: {os.path.join(out_dir, 'bias_vs_sun_merged.pdf')}")
    print(f"Wrote: {os.path.join(out_dir, 'bias_vs_hour_merged.pdf')}")


if __name__ == "__main__":
    main()
