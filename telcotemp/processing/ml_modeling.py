import joblib
from tensorflow.keras.models import load_model


def temperature_predict(df, scaler_path, lstm_model_path, bias_offset):
    col_order = [
        "Temperature_MW",
        "sun",
        "Hour",
        "Day",
        "Signal",
        "Azimuth",
        "Latitude",
        "Longitude",
        "Technology",
        "Elevation",
    ]

    df = df.copy()

    X = df[col_order]
    scaler = joblib.load(scaler_path)
    X_scaled = scaler.transform(X)
    X_reshaped = X_scaled.reshape((X_scaled.shape[0], X_scaled.shape[1], 1))

    model = load_model(lstm_model_path, compile=False)
    predicted = model.predict(X_reshaped, verbose=0).flatten()

    # Global bias correction (°C): Predicted_Temperature = model_pred + bias_offset
    predicted = predicted + float(bias_offset)

    df["Predicted_Temperature"] = predicted

    # Keep 10-minute timestamps (no hourly aggregation)
    out_cols = [
        "Time",
        "IP",
        "Latitude",
        "Longitude",
        "Technology",
        "Side",
        "Elevation",
        "Link_ID",
        "sun",
        "Hour",
        "Predicted_Temperature",
    ]
    df = df[out_cols]

    df = df.groupby(
        [
            "Time",
            "Link_ID",
            "Side",
            "IP",
            "Latitude",
            "Longitude",
            "Technology",
            "Elevation",
            "sun",
            "Hour",
        ],
        as_index=False,
    )["Predicted_Temperature"].median()

    return df
