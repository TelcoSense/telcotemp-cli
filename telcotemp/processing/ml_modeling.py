import joblib
from tensorflow.keras.models import load_model


def temperature_predict(df, scaler_path, lstm_model_path):
    """
    Predict temperature using LSTM model (CML mode only).

    :param df: DataFrame with features
    :param scaler_path: Path to scaler joblib file
    :param lstm_model_path: Path to LSTM model keras file
    :return: DataFrame with Predicted_Temperature column
    """
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

    X = df[col_order]
    scaler = joblib.load(scaler_path)
    X_scaled = scaler.transform(X)
    X_reshaped = X_scaled.reshape((X_scaled.shape[0], X_scaled.shape[1], 1))

    model = load_model(lstm_model_path, compile=False)
    predicted_temperatures = model.predict(X_reshaped, verbose=0).flatten()

    df["Predicted_Temperature"] = predicted_temperatures
    df = (
        df.groupby(
            [
                "Hour",
                "IP",
                "Latitude",
                "Longitude",
                "Technology",
                "Side",
                "Elevation",
                "Link_ID",
            ]
        )["Predicted_Temperature"]
        .median()
        .reset_index()
    )

    return df
