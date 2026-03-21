# TelcoTemp

TelcoTemp is a CLI application for generating hourly air-temperature maps from:

- CML data from microwave links
- meteorological station data
- or both together in combined mode

The repository contains the full runtime pipeline: data fetch, metadata enrichment, feature preparation, neural-network inference for CML, spatial interpolation, PNG export, optional InfluxDB write-back, and logging/diagnostics.

## What The Repo Does

At a high level, TelcoTemp turns time-series measurements into hourly temperature surfaces over the Czech Republic.

- In `cml` mode, TelcoTemp fetches microwave-link measurements, enriches them with link metadata, predicts air temperature with a PyTorch LSTM, merges nearby endpoints into roof-level interpolation points, and renders hourly maps.
- In `meteo` mode, it fetches station temperatures directly and interpolates them into maps.
- In `combined` mode, it runs both pipelines side by side.

The default output is a transparent PNG map per processed hour. Optional diagnostics and optional InfluxDB write-back are also supported.

## Main Pipeline

### CML pipeline

1. Read CML measurements from InfluxDB.
2. Load link metadata from MariaDB/MySQL.
3. Build runtime features such as daylight flag, hour/day features, coordinates, and altitude/elevation.
4. Create rolling sequences for each CML endpoint or `cml_id` group.
5. Run the configured PyTorch LSTM checkpoint.
6. Merge nearby endpoints into roof-level interpolation points.
7. Deduplicate nearly identical coordinates.
8. Interpolate temperature over the output grid.
9. Save the map and optionally write predicted point values back to InfluxDB.

### Meteo pipeline

1. Read station measurements from InfluxDB.
2. Enrich with station metadata.
3. Transform coordinates.
4. Interpolate temperature over the output grid.
5. Save the map.

## Neural Network Runtime

The current runtime is self-contained inside this repository. It does not need the training repository at inference time.

- Model inference is implemented in `telcotemp/neural/pytorch_sequence.py`.
- Runtime model settings are read from `[ml]` in `configs/config.ini`.
- The model checkpoint and scaler bundle are loaded from `telcotemp/neural/artifacts/`.
- Technology labels are configured in `[ml]` and matched against CML metadata.

See `telcotemp/neural/artifacts/README.md` for the expected artifact layout.

## Repository Structure

Important directories:

- `telcotemp/core/` - configuration, logging, initialization
- `telcotemp/data_sources/` - Influx readers and metadata-backed preprocessing for CML and meteo
- `telcotemp/processing/` - main orchestration and ML prediction entrypoints
- `telcotemp/neural/` - PyTorch sequence runtime and local model artifacts
- `telcotemp/geo/` - interpolation and geographic helpers
- `telcotemp/storage/` - file saving and optional InfluxDB write-back
- `telcotemp/visualization/` - map rendering
- `telcotemp/utils/` - diagnostics, cleanup, time helpers
- `configs/` - runtime configuration
- `env_info/` - dependency definitions for pip/conda

## Configuration

The main runtime configuration is `configs/config.ini`.

Key sections:

- `[ml]` - CML model artifact path, technologies, sequence length, model hyperparameters, roof merge settings
- `[influx_read]` / `[influx_write]` - CML data source and optional output sink
- `[influx]` - meteo data source
- `[mysql]` - CML and station metadata database
- `[grid]` - interpolation grid resolution
- `[interpolation]` - kriging/regression settings
- `[visualization]` - color scale behavior
- `[logging]` - `app.log` location and format
- `[diagnostics]` - bias-report output

Copy `configs/config.ini.dist` to `configs/config.ini` and fill in the environment-specific values before running.

## Running The App

Install dependencies with either `env_info/requirements.txt` or one of the conda environment files in `env_info/`.

```bash
pip install -r env_info/requirements.txt
```

Examples:

```bash
# Combined mode, continuous processing
python run.py

# Combined mode, process the last week
python run.py --first_run

# Combined mode, explicit time range
python run.py --start_time "2026-01-01 00:00" --end_time "2026-01-02 00:00"

# CML only
python run.py --mode cml --start_time "2026-01-01 00:00" --end_time "2026-01-02 00:00"

# Meteo only
python run.py --mode meteo --start_time "2026-01-01 00:00" --end_time "2026-01-02 00:00"

# Combined mode with source filters
python run.py --cml_filter_ids "123,456" --meteo_filter_ids "LKCB,LKKV"
```

## Outputs

Typical outputs are:

- `outputs_cml/` - CML-based hourly PNG maps
- `outputs_meteo/` - meteo-based hourly PNG maps
- `saved_grids/` - optional saved grid arrays when enabled
- `outputs_diagnostics/` - bias CSVs and plots when diagnostics are enabled
- `app.log` - backend runtime log

In CML mode, predicted point values can also be written to InfluxDB if `[influx_write] enable_write = true`.

## Notes On CML Inference

The CML neural-network path is sensitive to consistency between:

- checkpoint architecture
- scaler bundle
- configured technologies
- sequence grouping
- sampling interval

The runtime currently supports `group_col = cml_id`, where `cml_id` is interpreted as `Link_ID + "_" + Side`, matching the dataset-preparation pipeline used for training.

## Diagnostics And Logging

- The main runtime log is `app.log`.
- CML runs log both prepared-row counts and predicted-row counts before interpolation.
- Diagnostics can export per-hour bias reports when enabled in `[diagnostics]`.

## Acknowledgements

This work was financed through the project "Spatial Air Temperature Monitoring Using Microwave Links Data" (SS07020434), which is co-funded with state support from the Technology Agency of the Czech Republic under the Environment for Life Programme and further funded within the National Recovery Plan from the European Recovery and Resilience Facility.

<p align="center">
  <img src="assets/tacr.png" alt="Technology Agency of the Czech Republic" height="64" />
  &nbsp;&nbsp;&nbsp;&nbsp;
  <img src="assets/eu.png" alt="European Union" height="64" />
</p>
