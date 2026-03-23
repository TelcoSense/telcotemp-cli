Place exported model artifacts here, for example:

`telcotemp/neural/artifacts/current/`

Recommended files:
- `best.pt`
- `scaler_bundle.json`

The preferred setup is to keep model metadata in `[ml]` in `configs/config.ini`, for example:

```ini
[ml]
artifact_dir = telcotemp/neural/artifacts/current
checkpoint_path = best.pt
scaler_bundle_path = scaler_bundle.json
technologies = ["1s10", "ceragon_ip_20", "summit", "summit_bt"]
seq_len = 36
sample_minutes = 10
model_type = lstm
hidden_size = 96
num_layers = 4
dropout = 0.3
technology_embed_dim = 8
mlp_hidden_size = 96
head_activation = gelu
use_layer_norm = false
bidirectional = true
temporal_readout = last
```

Optional compatibility fallbacks:
- `prepare_summary.json` can be used to read`technologies` from a file instead of config.
- `artifact.yaml` or `artifact.json` can be used, but it is no longer required for the normal telcotemp setup.
