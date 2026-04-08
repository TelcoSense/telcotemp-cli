from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

LOGGER = logging.getLogger("backend_logger")
_LOGGED_RUNTIME_SIGNATURES: set[tuple[str, ...]] = set()

SEQ_FEATURE_COLUMNS = [
    "Temperature_MW",
    "sun",
    "hour_sin",
    "hour_cos",
    "day_sin",
    "day_cos",
]
STATIC_FEATURE_COLUMNS = ["Azimuth", "Elevation"]
OUTPUT_COLUMNS = [
    "Time",
    "IP",
    "Latitude",
    "Longitude",
    "X",
    "Y",
    "Technology",
    "Side",
    "Elevation",
    "Link_ID",
    "sun",
    "Hour",
]
REQUIRED_INFERENCE_COLUMNS = [
    "Time",
    "__sequence_group",
    "Temperature_MW",
    "sun",
    "Hour",
    "Day",
    "Azimuth",
    "Elevation",
    "Technology",
]


@dataclass
class StandardScaler:
    columns: list[str]
    mean: np.ndarray
    scale: np.ndarray

    @classmethod
    def from_dict(cls, payload: dict) -> "StandardScaler":
        return cls(
            columns=[str(x) for x in payload["columns"]],
            mean=np.asarray(payload["mean"], dtype=np.float32),
            scale=np.asarray(payload["scale"], dtype=np.float32),
        )

    def transform(self, values: np.ndarray) -> np.ndarray:
        arr = np.asarray(values, dtype=np.float32)
        return (arr - self.mean) / self.scale

    def inverse_transform(self, values: np.ndarray) -> np.ndarray:
        arr = np.asarray(values, dtype=np.float32)
        return arr * self.scale + self.mean


def build_activation(name: str):
    import torch.nn as nn

    key = str(name).lower()
    if key == "relu":
        return nn.ReLU()
    if key == "gelu":
        return nn.GELU()
    if key == "silu":
        return nn.SiLU()
    if key == "tanh":
        return nn.Tanh()
    raise ValueError(f"Unsupported activation={name}")


def _normalize_technology_label(value) -> str:
    return str(value).strip().lower()


def _resolve_feature_columns(
    scaler: StandardScaler | None,
    default_columns: list[str],
    aliases: dict[str, str],
) -> list[str]:
    if scaler is None:
        return list(default_columns)

    resolved = []
    for column in scaler.columns:
        key = str(column).strip().lower()
        if key not in aliases:
            raise KeyError(f"Unsupported feature column in scaler bundle: {column}")
        resolved.append(aliases[key])
    return resolved


def _required_input_columns(bundle: SequenceRuntimeBundle) -> list[str]:
    required = {"Time", "__sequence_group", "Technology"}
    derived_requirements = {
        "hour_sin": "Hour",
        "hour_cos": "Hour",
        "day_sin": "Day",
        "day_cos": "Day",
    }

    for column in bundle.seq_feature_cols + bundle.static_feature_cols:
        required.add(derived_requirements.get(column, column))
    return sorted(required)


def _build_lstm_model_class():
    import torch
    from torch import nn

    class CMLTempLSTM(nn.Module):
        def __init__(
            self,
            seq_input_size: int,
            num_technologies: int,
            hidden_size: int = 64,
            num_layers: int = 2,
            dropout: float = 0.2,
            technology_embed_dim: int = 8,
            mlp_hidden_size: int = 64,
            head_activation: str = "relu",
            use_layer_norm: bool = False,
            bidirectional: bool = False,
            temporal_readout: str = "last",
        ):
            super().__init__()
            self.tech_emb = nn.Embedding(num_technologies, technology_embed_dim)
            self.bidirectional = bool(bidirectional)
            self.temporal_readout = str(temporal_readout or "last").lower()
            if self.temporal_readout not in {"last", "mean"}:
                raise ValueError(
                    "Unsupported temporal_readout: "
                    f"{temporal_readout}. Expected 'last' or 'mean'."
                )
            self.lstm = nn.LSTM(
                input_size=seq_input_size,
                hidden_size=hidden_size,
                num_layers=num_layers,
                dropout=dropout if num_layers > 1 else 0.0,
                batch_first=True,
                bidirectional=self.bidirectional,
            )
            lstm_out_dim = hidden_size * (2 if self.bidirectional else 1)
            self.norm = (
                nn.LayerNorm(lstm_out_dim) if use_layer_norm else nn.Identity()
            )
            self.head = nn.Sequential(
                nn.Linear(lstm_out_dim + technology_embed_dim + 2, mlp_hidden_size),
                build_activation(head_activation),
                nn.Dropout(dropout),
                nn.Linear(mlp_hidden_size, 1),
            )

        def forward(
            self,
            x_seq: torch.Tensor,
            x_static: torch.Tensor,
            technology_idx: torch.Tensor,
        ) -> torch.Tensor:
            out, _ = self.lstm(x_seq)
            if self.temporal_readout == "mean":
                temporal_features = out.mean(dim=1)
            else:
                temporal_features = out[:, -1, :]
            h_last = self.norm(temporal_features)
            tech = self.tech_emb(technology_idx)
            x = torch.cat([h_last, x_static, tech], dim=1)
            return self.head(x).squeeze(1)

    return CMLTempLSTM


@dataclass
class SequenceRuntimeBundle:
    seq_len: int
    sample_minutes: int
    seq_feature_cols: list[str]
    static_feature_cols: list[str]
    tech_to_idx: dict[str, int]
    seq_scaler: StandardScaler | None
    static_scaler: StandardScaler | None
    target_scaler: StandardScaler | None
    model: object
    device: str
    configured_model_name: str
    resolved_model_name: str
    checkpoint_path: str
    checkpoint_model_name: str


@dataclass
class SequenceInferenceInputs:
    work: pd.DataFrame
    window_end_indices: np.ndarray
    x_seq: np.ndarray
    x_static: np.ndarray
    tech_idx: np.ndarray


def _read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _load_structured_file(path: Path) -> dict:
    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        return _load_yaml(path)
    return _read_json(path)


def _parse_optional_int(value):
    if value in (None, ""):
        return None
    return int(value)


def _parse_optional_float(value):
    if value in (None, ""):
        return None
    return float(value)


def _parse_optional_bool(value):
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _normalize_string_list(values) -> list[str]:
    if values in (None, ""):
        return []
    if isinstance(values, str):
        return [values]
    if not isinstance(values, (list, tuple)):
        raise TypeError(f"Expected a list of strings, got {type(values).__name__}.")
    return [str(item) for item in values]


def _resolve_path(base_dir: Path | None, raw_path: str | None, default_name: str) -> Path:
    candidate = raw_path or default_name
    path = Path(candidate).expanduser()
    if path.is_absolute() or base_dir is None:
        return path
    return base_dir / path


def _artifact_config_candidates(artifact_dir: Path) -> list[Path]:
    return [
        artifact_dir / "artifact.yaml",
        artifact_dir / "artifact.yml",
        artifact_dir / "artifact.json",
        artifact_dir / "runtime.yaml",
        artifact_dir / "runtime.yml",
        artifact_dir / "runtime.json",
    ]


def _load_artifact_config(artifact_dir: Path | None, artifact_config_path: str | None) -> dict:
    if artifact_config_path:
        path = Path(artifact_config_path).expanduser()
        if not path.is_absolute() and artifact_dir is not None:
            path = artifact_dir / path
        if not path.exists():
            raise FileNotFoundError(f"Artifact config file was not found: {path}")
        return _load_structured_file(path)

    if artifact_dir is None:
        return {}

    for candidate in _artifact_config_candidates(artifact_dir):
        if candidate.exists():
            return _load_structured_file(candidate)
    return {}


def _resolve_artifact_model_entry(
    artifact_cfg: dict,
    model_name: str | None,
) -> tuple[str | None, dict]:
    requested_model = (
        str(model_name).strip() if model_name not in (None, "") else ""
    )
    models_cfg = artifact_cfg.get("models")
    if isinstance(models_cfg, dict) and models_cfg:
        if requested_model:
            selected_name = requested_model
        else:
            default_model = artifact_cfg.get("default_model")
            if default_model not in (None, ""):
                selected_name = str(default_model).strip()
            elif len(models_cfg) == 1:
                selected_name = str(next(iter(models_cfg)))
            else:
                raise ValueError(
                    "Multiple artifact models are configured. Set [ml] model_name "
                    "or define default_model in artifact.yaml/json."
                )

        if selected_name not in models_cfg:
            raise KeyError(
                f"Model '{selected_name}' was not found in artifact config. "
                f"Available models: {sorted(models_cfg)}"
            )

        selected_cfg = models_cfg.get(selected_name) or {}
        if not isinstance(selected_cfg, dict):
            raise TypeError(
                f"Artifact config entry for model '{selected_name}' must be a mapping."
            )
        return selected_name, dict(selected_cfg)

    legacy_model_cfg = artifact_cfg.get("model", {})
    if legacy_model_cfg in (None, ""):
        legacy_model_cfg = {}
    if not isinstance(legacy_model_cfg, dict):
        raise TypeError("Artifact config field 'model' must be a mapping.")
    return (requested_model or None), dict(legacy_model_cfg)


def _artifact_window_settings(ml_cfg: dict) -> dict | None:
    artifact_dir_raw = ml_cfg.get("artifact_dir")
    artifact_dir = (
        Path(artifact_dir_raw).expanduser()
        if artifact_dir_raw not in (None, "")
        else None
    )
    artifact_cfg = _load_artifact_config(
        artifact_dir=artifact_dir,
        artifact_config_path=ml_cfg.get("artifact_config_path"),
    )
    if artifact_dir is None and not artifact_cfg:
        return None

    artifact_model_name, artifact_model_cfg = _resolve_artifact_model_entry(
        artifact_cfg, ml_cfg.get("model_name")
    )
    window_cfg = artifact_cfg.get("window", {})
    if not isinstance(window_cfg, dict):
        raise TypeError("Artifact config field 'window' must be a mapping.")
    model_window_cfg = artifact_model_cfg.get("window", {})
    if model_window_cfg in (None, ""):
        model_window_cfg = {}
    if not isinstance(model_window_cfg, dict):
        raise TypeError(
            f"Artifact config window for model '{artifact_model_name}' must be a mapping."
        )
    seq_len = _parse_optional_int(ml_cfg.get("seq_len"))
    sample_minutes = _parse_optional_int(ml_cfg.get("sample_minutes"))

    if seq_len is None:
        seq_len = _parse_optional_int(model_window_cfg.get("seq_len"))
    if seq_len is None:
        seq_len = _parse_optional_int(artifact_model_cfg.get("seq_len"))
    if seq_len is None:
        seq_len = _parse_optional_int(window_cfg.get("seq_len"))
    if sample_minutes is None:
        sample_minutes = _parse_optional_int(model_window_cfg.get("sample_minutes"))
    if sample_minutes is None:
        sample_minutes = _parse_optional_int(artifact_model_cfg.get("sample_minutes"))
    if sample_minutes is None:
        sample_minutes = _parse_optional_int(window_cfg.get("sample_minutes"))

    if seq_len is None or sample_minutes is None:
        missing = []
        if seq_len is None:
            missing.append("seq_len")
        if sample_minutes is None:
            missing.append("sample_minutes")
        raise ValueError(
            "Missing artifact window settings: "
            + ", ".join(missing)
            + ". Set them in [ml] or include them in artifact.yaml/json."
        )
    return {
        "artifact_dir": artifact_dir,
        "artifact_cfg": artifact_cfg,
        "artifact_model_name": artifact_model_name,
        "artifact_model_cfg": artifact_model_cfg,
        "seq_len": seq_len,
        "sample_minutes": sample_minutes,
    }


def resolve_window_settings(ml_cfg: dict) -> tuple[int, int]:
    artifact_settings = _artifact_window_settings(ml_cfg)
    if artifact_settings is not None:
        return artifact_settings["seq_len"], artifact_settings["sample_minutes"]

    nn_config_path = ml_cfg.get("nn_config_path")
    if not nn_config_path:
        raise ValueError(
            "No model artifact window settings were found. Configure artifact_dir/seq_len "
            "or provide nn_config_path."
        )
    cfg_path = Path(nn_config_path).expanduser()
    cfg = _load_yaml(cfg_path)
    return int(cfg["window"]["seq_len"]), int(cfg["window"]["sample_minutes"])


def _resolve_repo_paths(
    nn_config_path: str,
    model_name: str,
    checkpoint_path: str | None,
    prepare_summary_path: str | None,
    scaler_bundle_path: str | None,
) -> tuple[dict, Path, Path, Path]:
    cfg_path = Path(nn_config_path).expanduser()
    cfg = _load_yaml(cfg_path)
    repo_root = cfg_path.resolve().parent.parent

    def resolve_from_repo(raw_path: str) -> Path:
        p = Path(raw_path).expanduser()
        return p if p.is_absolute() else repo_root / p

    prepared_dir = resolve_from_repo(cfg["paths"]["prepared_dir"])
    models_dir = resolve_from_repo(cfg["paths"]["models_dir"])

    ckpt = (
        Path(checkpoint_path).expanduser()
        if checkpoint_path
        else models_dir / model_name / "best.pt"
    )
    summary = (
        Path(prepare_summary_path).expanduser()
        if prepare_summary_path
        else prepared_dir / "prepare_summary.json"
    )
    scalers = (
        Path(scaler_bundle_path).expanduser()
        if scaler_bundle_path
        else prepared_dir / "scaler_bundle.json"
    )
    return cfg, ckpt, summary, scalers


def _resolve_artifact_paths_and_config(
    ml_cfg: dict,
) -> tuple[dict, Path, Path | None, Path, dict]:
    artifact_settings = _artifact_window_settings(ml_cfg)
    if artifact_settings is None:
        raise ValueError("Artifact settings are not configured.")

    artifact_dir = artifact_settings["artifact_dir"]
    artifact_cfg = artifact_settings["artifact_cfg"]
    artifact_model_name = artifact_settings["artifact_model_name"]
    model_cfg = artifact_settings["artifact_model_cfg"].copy()

    overrides = {
        "type": ml_cfg.get("model_type"),
        "hidden_size": _parse_optional_int(ml_cfg.get("hidden_size")),
        "num_layers": _parse_optional_int(ml_cfg.get("num_layers")),
        "dropout": _parse_optional_float(ml_cfg.get("dropout")),
        "technology_embed_dim": _parse_optional_int(ml_cfg.get("technology_embed_dim")),
        "mlp_hidden_size": _parse_optional_int(ml_cfg.get("mlp_hidden_size")),
        "head_activation": ml_cfg.get("head_activation"),
        "use_layer_norm": _parse_optional_bool(ml_cfg.get("use_layer_norm")),
        "bidirectional": _parse_optional_bool(ml_cfg.get("bidirectional")),
        "temporal_readout": ml_cfg.get("temporal_readout"),
    }
    for key, value in overrides.items():
        if value not in (None, ""):
            model_cfg[key] = value

    if str(model_cfg.get("type", "lstm")).lower() != "lstm":
        raise ValueError("Only LSTM artifacts are supported in telcotemp runtime.")

    required_keys = [
        "hidden_size",
        "num_layers",
        "dropout",
        "technology_embed_dim",
        "mlp_hidden_size",
    ]
    missing = [key for key in required_keys if model_cfg.get(key) in (None, "")]
    if missing:
        raise ValueError(
            "Missing model parameters for artifact runtime: "
            + ", ".join(missing)
            + ". Set them in [ml] or include them in artifact.yaml/json."
        )

    paths_cfg = artifact_cfg.get("paths", {})
    if not isinstance(paths_cfg, dict):
        raise TypeError("Artifact config field 'paths' must be a mapping.")

    model_paths_cfg = model_cfg.get("paths", {})
    if model_paths_cfg in (None, ""):
        model_paths_cfg = {}
    if not isinstance(model_paths_cfg, dict):
        raise TypeError(
            f"Artifact config paths for model '{artifact_model_name}' must be a mapping."
        )

    checkpoints_cfg = artifact_cfg.get("checkpoints", {})
    if checkpoints_cfg in (None, ""):
        checkpoints_cfg = {}
    if not isinstance(checkpoints_cfg, dict):
        raise TypeError("Artifact config field 'checkpoints' must be a mapping.")

    checkpoint_raw = ml_cfg.get("checkpoint_path")
    if checkpoint_raw in (None, ""):
        checkpoint_raw = (
            model_paths_cfg.get("checkpoint")
            or model_cfg.get("checkpoint")
            or model_cfg.get("checkpoint_path")
        )
    if checkpoint_raw in (None, "") and artifact_model_name:
        checkpoint_raw = checkpoints_cfg.get(artifact_model_name)
    checkpoint_path = _resolve_path(
        artifact_dir,
        checkpoint_raw or paths_cfg.get("checkpoint"),
        "best.pt",
    )

    summary_raw = ml_cfg.get("prepare_summary_path")
    if summary_raw in (None, ""):
        summary_raw = (
            model_paths_cfg.get("prepare_summary")
            or model_cfg.get("prepare_summary")
            or model_cfg.get("prepare_summary_path")
            or paths_cfg.get("prepare_summary")
        )
    summary_path = (
        _resolve_path(artifact_dir, summary_raw, "prepare_summary.json")
        if summary_raw not in (None, "")
        else None
    )

    scalers_raw = ml_cfg.get("scaler_bundle_path")
    if scalers_raw in (None, ""):
        scalers_raw = (
            model_paths_cfg.get("scaler_bundle")
            or model_cfg.get("scaler_bundle")
            or model_cfg.get("scaler_bundle_path")
            or paths_cfg.get("scaler_bundle")
        )
    scalers_path = _resolve_path(artifact_dir, scalers_raw, "scaler_bundle.json")

    return artifact_settings, checkpoint_path, summary_path, scalers_path, model_cfg


@lru_cache(maxsize=8)
def load_runtime_bundle(
    artifact_dir: str | None,
    artifact_config_path: str | None,
    nn_config_path: str | None,
    model_name: str,
    checkpoint_path: str | None,
    prepare_summary_path: str | None,
    scaler_bundle_path: str | None,
    technologies: tuple[str, ...],
    seq_len: int | None,
    sample_minutes: int | None,
    model_type: str | None,
    hidden_size: int | None,
    num_layers: int | None,
    dropout: float | None,
    technology_embed_dim: int | None,
    mlp_hidden_size: int | None,
    head_activation: str | None,
    use_layer_norm: bool | None,
    bidirectional: bool | None,
    temporal_readout: str | None,
    device: str,
) -> SequenceRuntimeBundle:
    import torch

    checkpoint = None
    configured_model_name = str(model_name or "").strip()
    resolved_model_name = configured_model_name
    if artifact_dir or artifact_config_path:
        ml_cfg = {
            "artifact_dir": artifact_dir,
            "artifact_config_path": artifact_config_path,
            "model_name": model_name,
            "checkpoint_path": checkpoint_path,
            "prepare_summary_path": prepare_summary_path,
            "scaler_bundle_path": scaler_bundle_path,
            "technologies": list(technologies),
            "seq_len": seq_len,
            "sample_minutes": sample_minutes,
            "model_type": model_type,
            "hidden_size": hidden_size,
            "num_layers": num_layers,
            "dropout": dropout,
            "technology_embed_dim": technology_embed_dim,
            "mlp_hidden_size": mlp_hidden_size,
            "head_activation": head_activation,
            "use_layer_norm": use_layer_norm,
            "bidirectional": bidirectional,
            "temporal_readout": temporal_readout,
        }
        artifact_settings, ckpt_path, summary_path, scalers_path, model_cfg = (
            _resolve_artifact_paths_and_config(ml_cfg)
        )
        resolved_seq_len = artifact_settings["seq_len"]
        resolved_sample_minutes = artifact_settings["sample_minutes"]
        artifact_cfg = artifact_settings["artifact_cfg"]
        resolved_model_name = str(
            artifact_settings.get("artifact_model_name") or configured_model_name or ""
        ).strip()
    else:
        if not nn_config_path:
            raise ValueError(
                "No local artifact configuration was found and nn_config_path is missing."
            )
        resolved_model_name = configured_model_name or "lstm_gelu"
        cfg, ckpt_path, summary_path, scalers_path = _resolve_repo_paths(
            nn_config_path=nn_config_path,
            model_name=resolved_model_name,
            checkpoint_path=checkpoint_path,
            prepare_summary_path=prepare_summary_path,
            scaler_bundle_path=scaler_bundle_path,
        )

        model_cfg = (cfg.get("models") or {}).get(resolved_model_name)
        if not model_cfg:
            raise KeyError(
                f"Model '{resolved_model_name}' was not found in training config {nn_config_path}."
            )
        if str(model_cfg.get("type", "lstm")).lower() != "lstm":
            raise ValueError(
                f"Model '{resolved_model_name}' is not an LSTM. Only LSTM checkpoints are supported."
            )
        resolved_seq_len = int(cfg["window"]["seq_len"])
        resolved_sample_minutes = int(cfg["window"]["sample_minutes"])
        artifact_cfg = {}

    if configured_model_name and resolved_model_name:
        if configured_model_name != resolved_model_name:
            raise ValueError(
                "Configured model name does not match the resolved runtime model: "
                f"configured='{configured_model_name}', "
                f"resolved='{resolved_model_name}'."
            )

    if technologies:
        techs = [_normalize_technology_label(item) for item in technologies]
    else:
        techs = _normalize_string_list(model_cfg.get("technologies"))
        if not techs:
            techs = _normalize_string_list(artifact_cfg.get("technologies"))
        if not techs and summary_path is not None:
            prepare_summary = _read_json(summary_path)
            techs = _normalize_string_list(prepare_summary.get("technologies"))
        if not techs:
            checkpoint = torch.load(ckpt_path, map_location="cpu")
            checkpoint_vocab = (
                checkpoint.get("technology_vocab", [])
                if isinstance(checkpoint, dict)
                else []
            )
            techs = _normalize_string_list(checkpoint_vocab)
        if not techs:
            raise ValueError(
                "No technologies were configured. Set [ml] technologies, add technologies "
                "to artifact.yaml/json, provide prepare_summary_path, or store "
                "technology_vocab in the checkpoint."
            )
        techs = [_normalize_technology_label(item) for item in techs]
    tech_to_idx = {name: idx for idx, name in enumerate(techs)}

    seq_scaler = None
    static_scaler = None
    target_scaler = None
    if scalers_path.exists():
        scaler_payload = _read_json(scalers_path)
        seq_scaler = StandardScaler.from_dict(scaler_payload["seq"])
        static_scaler = StandardScaler.from_dict(scaler_payload["static"])
        target_scaler = StandardScaler.from_dict(scaler_payload["target"])

    requested_device = str(device or "cpu").lower()
    resolved_device = (
        "cuda"
        if requested_device == "auto" and torch.cuda.is_available()
        else requested_device
    )

    CMLTempLSTM = _build_lstm_model_class()
    seq_feature_cols = _resolve_feature_columns(
        seq_scaler,
        default_columns=list(SEQ_FEATURE_COLUMNS),
        aliases={
            "temp_unit": "Temperature_MW",
            "temperature_mw": "Temperature_MW",
            "sun": "sun",
            "hour_sin": "hour_sin",
            "hour_cos": "hour_cos",
            "day_sin": "day_sin",
            "day_cos": "day_cos",
        },
    )
    static_feature_cols = _resolve_feature_columns(
        static_scaler,
        default_columns=list(STATIC_FEATURE_COLUMNS),
        aliases={
            "azimuth": "Azimuth",
            "altitude": "Altitude",
            "elevation": "Elevation",
        },
    )
    model = CMLTempLSTM(
        seq_input_size=len(seq_feature_cols),
        num_technologies=len(tech_to_idx),
        hidden_size=int(model_cfg["hidden_size"]),
        num_layers=int(model_cfg["num_layers"]),
        dropout=float(model_cfg["dropout"]),
        technology_embed_dim=int(model_cfg["technology_embed_dim"]),
        mlp_hidden_size=int(model_cfg["mlp_hidden_size"]),
        head_activation=str(model_cfg.get("head_activation", "relu")),
        use_layer_norm=bool(model_cfg.get("use_layer_norm", False)),
        bidirectional=bool(model_cfg.get("bidirectional", False)),
        temporal_readout=str(model_cfg.get("temporal_readout", "last")),
    )

    if checkpoint is None:
        checkpoint = torch.load(ckpt_path, map_location=resolved_device)
    checkpoint_model_name = ""
    if isinstance(checkpoint, dict):
        checkpoint_model_name = str(checkpoint.get("model_name") or "").strip()
    state_dict = checkpoint["model_state"] if "model_state" in checkpoint else checkpoint
    model.load_state_dict(state_dict)
    model.to(resolved_device)
    model.eval()

    LOGGER.info(
        "[CML][model] loaded configured_model=%s resolved_model=%s checkpoint=%s checkpoint_model=%s seq_len=%d sample_minutes=%d device=%s",
        configured_model_name or "<default>",
        resolved_model_name or "<legacy>",
        Path(ckpt_path).name,
        checkpoint_model_name or "<missing>",
        resolved_seq_len,
        resolved_sample_minutes,
        resolved_device,
    )

    return SequenceRuntimeBundle(
        seq_len=resolved_seq_len,
        sample_minutes=resolved_sample_minutes,
        seq_feature_cols=seq_feature_cols,
        static_feature_cols=static_feature_cols,
        tech_to_idx=tech_to_idx,
        seq_scaler=seq_scaler,
        static_scaler=static_scaler,
        target_scaler=target_scaler,
        model=model,
        device=resolved_device,
        configured_model_name=configured_model_name,
        resolved_model_name=resolved_model_name,
        checkpoint_path=str(ckpt_path),
        checkpoint_model_name=checkpoint_model_name,
    )


def _build_sequence_group(df: pd.DataFrame, group_col: str) -> pd.Series:
    if group_col in {"Link_ID_Side", "cml_id"}:
        return df["Link_ID"].astype(str) + "_" + df["Side"].astype(str)
    if group_col not in df.columns:
        raise KeyError(f"Sequence group column '{group_col}' not found in dataframe.")
    return df[group_col].astype(str)


def _find_window_end_indices(
    df: pd.DataFrame,
    group_col: str,
    time_col: str,
    seq_len: int,
    sample_minutes: int,
) -> np.ndarray:
    if len(df) < seq_len:
        return np.empty(0, dtype=np.int64)

    expected_delta_ns = pd.Timedelta(minutes=sample_minutes).value
    group_values = df[group_col].to_numpy(copy=False)
    time_values = df[time_col].array.asi8

    transition_ok = (
        (group_values[1:] == group_values[:-1])
        & (np.diff(time_values) == expected_delta_ns)
    )
    pair_positions = np.arange(transition_ok.size, dtype=np.int64)
    last_break = np.maximum.accumulate(np.where(~transition_ok, pair_positions, -1))
    consecutive_pairs = np.where(transition_ok, pair_positions - last_break, 0)

    run_lengths = np.zeros(len(df), dtype=np.int64)
    run_lengths[1:] = consecutive_pairs
    return np.flatnonzero(run_lengths >= (seq_len - 1)).astype(np.int64)


def _resolve_inference_batch_size(ml_cfg: dict) -> int:
    batch_size = _parse_optional_int(ml_cfg.get("inference_batch_size"))
    return max(1, batch_size or 2048)


def _prepare_inference_inputs(
    df: pd.DataFrame,
    bundle: SequenceRuntimeBundle,
    ml_cfg: dict,
) -> SequenceInferenceInputs | None:
    work = df.copy()
    input_rows = len(work)
    work["__sequence_group"] = _build_sequence_group(
        work, ml_cfg.get("group_col", "IP")
    )
    required_input_columns = _required_input_columns(bundle)
    work = work.dropna(subset=required_input_columns).copy()
    if work.empty:
        LOGGER.warning(
            "[CML][predict] empty after required-column dropna; input_rows=%d, required=%s",
            input_rows,
            required_input_columns,
        )
        return None

    work["Technology"] = work["Technology"].astype(str)
    work["__technology_key"] = work["Technology"].map(_normalize_technology_label)
    work = work[work["__technology_key"].isin(bundle.tech_to_idx)].copy()
    if work.empty:
        LOGGER.warning(
            "[CML][predict] empty after technology filter; model_technologies=%s",
            sorted(bundle.tech_to_idx),
        )
        return None

    work["hour_sin"] = np.sin(2 * np.pi * work["Hour"].astype(float) / 24.0)
    work["hour_cos"] = np.cos(2 * np.pi * work["Hour"].astype(float) / 24.0)
    work["day_sin"] = np.sin(2 * np.pi * work["Day"].astype(float) / 365.25)
    work["day_cos"] = np.cos(2 * np.pi * work["Day"].astype(float) / 365.25)
    work = work.sort_values(["__sequence_group", "Time"]).reset_index(drop=True)

    seq_array = work[bundle.seq_feature_cols].to_numpy(dtype=np.float32)
    static_array = work[bundle.static_feature_cols].to_numpy(dtype=np.float32)
    if bundle.seq_scaler is not None:
        seq_array = bundle.seq_scaler.transform(seq_array)
    if bundle.static_scaler is not None:
        static_array = bundle.static_scaler.transform(static_array)

    tech_idx_array = work["__technology_key"].map(bundle.tech_to_idx).to_numpy(
        dtype=np.int64
    )
    window_end_indices = _find_window_end_indices(
        work,
        group_col="__sequence_group",
        time_col="Time",
        seq_len=bundle.seq_len,
        sample_minutes=bundle.sample_minutes,
    )
    if len(window_end_indices) == 0:
        LOGGER.warning(
            "[CML][predict] no valid sequence windows; rows_after_filters=%d, seq_len=%d, sample_minutes=%d",
            len(work),
            bundle.seq_len,
            bundle.sample_minutes,
        )
        return None

    window_starts = window_end_indices - bundle.seq_len + 1
    window_offsets = np.arange(bundle.seq_len, dtype=np.int64)
    x_seq = seq_array[window_starts[:, None] + window_offsets[None, :]].astype(
        np.float32,
        copy=False,
    )
    x_static = static_array[window_end_indices].astype(np.float32, copy=False)
    tech_idx = tech_idx_array[window_end_indices]

    return SequenceInferenceInputs(
        work=work,
        window_end_indices=window_end_indices,
        x_seq=x_seq,
        x_static=x_static,
        tech_idx=tech_idx,
    )


def _predict_batches(
    bundle: SequenceRuntimeBundle,
    inputs: SequenceInferenceInputs,
    batch_size: int,
) -> np.ndarray:
    import torch

    total = len(inputs.window_end_indices)
    predicted = np.empty(total, dtype=np.float32)

    with torch.inference_mode():
        for start in range(0, total, batch_size):
            stop = min(start + batch_size, total)
            y_scaled = (
                bundle.model(
                    torch.from_numpy(
                        np.ascontiguousarray(inputs.x_seq[start:stop])
                    ).to(bundle.device),
                    torch.from_numpy(
                        np.ascontiguousarray(inputs.x_static[start:stop])
                    ).to(bundle.device),
                    torch.from_numpy(
                        np.ascontiguousarray(inputs.tech_idx[start:stop])
                    ).to(bundle.device),
                )
                .detach()
                .cpu()
                .numpy()
            )
            predicted[start:stop] = y_scaled.reshape(-1)

    if bundle.target_scaler is not None:
        predicted = bundle.target_scaler.inverse_transform(
            predicted.reshape(-1, 1)
        ).reshape(-1)
    return predicted


def _build_prediction_output(
    inputs: SequenceInferenceInputs,
    predicted: np.ndarray,
    prediction_start=None,
    prediction_end=None,
) -> pd.DataFrame:
    out = inputs.work.loc[inputs.window_end_indices, OUTPUT_COLUMNS].copy()
    out["Predicted_Temperature"] = predicted

    if prediction_start is not None and prediction_end is not None:
        out = out[
            (out["Time"] >= prediction_start) & (out["Time"] < prediction_end)
        ].copy()
        if out.empty:
            return _empty_prediction_frame()

    return out.groupby(
        [
            "Time",
            "Link_ID",
            "Side",
            "IP",
            "Latitude",
            "Longitude",
            "X",
            "Y",
            "Technology",
            "Elevation",
            "sun",
            "Hour",
        ],
        as_index=False,
    )["Predicted_Temperature"].median()


def _empty_prediction_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "Time",
            "IP",
            "Latitude",
            "Longitude",
            "X",
            "Y",
            "Technology",
            "Side",
            "Elevation",
            "Link_ID",
            "sun",
            "Hour",
            "Predicted_Temperature",
        ]
    )


def predict_temperature_sequence(
    df: pd.DataFrame,
    ml_cfg: dict,
    prediction_start=None,
    prediction_end=None,
) -> pd.DataFrame:
    if df.empty:
        return _empty_prediction_frame()

    bundle = load_runtime_bundle(
        artifact_dir=ml_cfg.get("artifact_dir"),
        artifact_config_path=ml_cfg.get("artifact_config_path"),
        nn_config_path=ml_cfg.get("nn_config_path"),
        model_name=ml_cfg["model_name"],
        checkpoint_path=ml_cfg.get("checkpoint_path"),
        prepare_summary_path=ml_cfg.get("prepare_summary_path"),
        scaler_bundle_path=ml_cfg.get("scaler_bundle_path"),
        technologies=tuple(ml_cfg.get("technologies", [])),
        seq_len=_parse_optional_int(ml_cfg.get("seq_len")),
        sample_minutes=_parse_optional_int(ml_cfg.get("sample_minutes")),
        model_type=ml_cfg.get("model_type"),
        hidden_size=_parse_optional_int(ml_cfg.get("hidden_size")),
        num_layers=_parse_optional_int(ml_cfg.get("num_layers")),
        dropout=_parse_optional_float(ml_cfg.get("dropout")),
        technology_embed_dim=_parse_optional_int(
            ml_cfg.get("technology_embed_dim")
        ),
        mlp_hidden_size=_parse_optional_int(ml_cfg.get("mlp_hidden_size")),
        head_activation=ml_cfg.get("head_activation"),
        use_layer_norm=_parse_optional_bool(ml_cfg.get("use_layer_norm")),
        bidirectional=_parse_optional_bool(ml_cfg.get("bidirectional")),
        temporal_readout=ml_cfg.get("temporal_readout"),
        device=ml_cfg.get("device", "cpu"),
    )

    runtime_signature = (
        bundle.configured_model_name,
        bundle.resolved_model_name,
        bundle.checkpoint_path,
        bundle.checkpoint_model_name,
        str(bundle.seq_len),
        str(bundle.sample_minutes),
        bundle.device,
    )
    if runtime_signature not in _LOGGED_RUNTIME_SIGNATURES:
        _LOGGED_RUNTIME_SIGNATURES.add(runtime_signature)
        LOGGER.info(
            "[CML][model] active configured_model=%s resolved_model=%s checkpoint=%s checkpoint_model=%s",
            bundle.configured_model_name or "<default>",
            bundle.resolved_model_name or "<legacy>",
            Path(bundle.checkpoint_path).name,
            bundle.checkpoint_model_name or "<missing>",
        )

    inputs = _prepare_inference_inputs(df=df, bundle=bundle, ml_cfg=ml_cfg)
    if inputs is None:
        return _empty_prediction_frame()

    predicted = _predict_batches(
        bundle=bundle,
        inputs=inputs,
        batch_size=_resolve_inference_batch_size(ml_cfg),
    )
    predicted = predicted + float(ml_cfg.get("bias_offset", 0.0))
    return _build_prediction_output(
        inputs=inputs,
        predicted=predicted,
        prediction_start=prediction_start,
        prediction_end=prediction_end,
    )
