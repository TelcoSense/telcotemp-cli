Place exported model artifacts here, for example:

`telcotemp/neural/artifacts/`

Recommended files:
- `artifact.yaml`
- `*.pt`
- `scaler_bundle.json`

The preferred setup is to keep model metadata inside `artifact.yaml` and only select
the desired model from `[ml]` in `configs/config.ini`, for example:

```ini
[ml]
artifact_dir = telcotemp/neural/artifacts
model_name = lstm_v6_final2
```

No architecture-specific fields such as `hidden_size`, `num_layers`, `dropout`,
`bidirectional`, or `temporal_readout` are needed in `[ml]` when using artifacts.
The same applies to `technologies` if you store them in `artifact.yaml/json` or in
checkpoint metadata.

Example `artifact.yaml`:

```yaml
default_model: lstm_v6_final2

window:
  seq_len: 72
  sample_minutes: 10

technologies:
  - 1s10
  - ceragon_ip_20
  - summit
  - summit_bt

paths:
  scaler_bundle: scaler_bundle.json

models:
  lstm_v6_final1:
    checkpoint: lstm_v6_final1.pt
    type: lstm
    hidden_size: 64
    num_layers: 2
    dropout: 0.2
    technology_embed_dim: 8
    mlp_hidden_size: 64
    head_activation: gelu
    use_layer_norm: false
    bidirectional: false
    temporal_readout: last
  lstm_v6_final2:
    checkpoint: lstm_v6_final2.pt
    type: lstm
    hidden_size: 96
    num_layers: 4
    dropout: 0.3
    technology_embed_dim: 8
    mlp_hidden_size: 96
    head_activation: gelu
    use_layer_norm: false
    bidirectional: true
    temporal_readout: last
```

Optional compatibility fallbacks:
- `prepare_summary.json` can be used to read `technologies` from a file instead of config.
- `artifact.yaml` or `artifact.json` can define one legacy `model:` block or multiple
  named `models:` entries.
- If no explicit technology list is configured, runtime also tries `technology_vocab`
  from the checkpoint itself.
