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
hidden_size = 64
num_layers = 2
dropout = 0.2
technology_embed_dim = 8
mlp_hidden_size = 64
head_activation = gelu
use_layer_norm = false
bidirectional = false
```

Optional compatibility fallbacks:
- `prepare_summary.json` can still be used if you prefer reading `technologies` from a file instead of config.
- `artifact.yaml` or `artifact.json` can still be used, but it is no longer required for the normal telcotemp setup.
