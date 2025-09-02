import logging
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
from astral.sun import sun
from astral import LocationInfo
import pytz

backend_logger = logging.getLogger('backend_logger')


def is_daylight(ts_utc, lat, lng, local_tz_str):
    if ts_utc.tzinfo is None or ts_utc.tzinfo.utcoffset(ts_utc) is None:
        raise ValueError("ts_utc must be tz-aware in UTC")

    local_tz = pytz.timezone(local_tz_str)
    local_date = ts_utc.astimezone(local_tz).date()
    loc = LocationInfo(timezone=local_tz_str, latitude=lat, longitude=lng)

    s = sun(loc.observer, date=local_date, tzinfo=local_tz)
    sunrise_utc = s["sunrise"].astimezone(pytz.UTC)
    sunset_utc = s["sunset"].astimezone(pytz.UTC)

    return 1 if sunrise_utc <= ts_utc <= sunset_utc else 0


def get_data(config):
    read_cfg = config.get_influx_config("read")
    loc = config.get_location()

    fields = read_cfg["fields"]
    field_temp = read_cfg["field_temperature"]
    field_sig = read_cfg["field_signal"]
    device_tag = read_cfg["tag_device"]
    measurements = read_cfg["measurements"]

    meas_filter = " or ".join([f'r["_measurement"] == "{m}"' for m in measurements])
    fields_filter = " or ".join([f'r["_field"] == "{f}"' for f in fields])

    try:
        with InfluxDBClient(url=read_cfg["url"], token=read_cfg["token"]) as client:
            query = f'''
                from(bucket: "{read_cfg["bucket"]}")
                  |> range(start: {read_cfg["range"]})
                  |> filter(fn: (r) => {meas_filter})
                  |> filter(fn: (r) => {fields_filter})
                  |> aggregateWindow(every: {read_cfg["window"]}, fn: mean)
                  |> group(columns: ["_measurement", "_field", "{device_tag}"])
                '''
            result = client.query_api().query(org=read_cfg["org"], query=query)

            data = [
                {
                    "Time": rec.get_time(),
                    "Measurement": rec.values["_field"],
                    "Value": rec.get_value(),
                    "Device": rec.values[device_tag],
                }
                for table in result for rec in table.records
            ]

            df = pd.DataFrame(data)
            if df.empty:
                backend_logger.info("Influx vrátil prázdná data.")
                return df

            df_pivot = df.pivot_table(
                index=["Time", "Device"], columns="Measurement", values="Value"
            ).reset_index()

            df_pivot["Time"] = pd.to_datetime(df_pivot["Time"], utc=True)
            df_pivot["Unix"] = df_pivot["Time"].astype("int64") // 10 ** 9
            if field_temp in df_pivot.columns:
                df_pivot.rename(columns={field_temp: "Temperature_MW"}, inplace=True)
            if field_sig in df_pivot.columns:
                df_pivot.rename(columns={field_sig: "Signal"}, inplace=True)

            df_pivot["sun"] = df_pivot["Time"].apply(
                lambda t: is_daylight(t, loc["lat"], loc["lng"], loc["tz"])
            )
            df_final = df_pivot.rename(columns={"Device": "IP"})
            return df_final

    except Exception as e:
        backend_logger.warning(f"Influx get_data selhal: {e}")

    return pd.DataFrame()


def write_predictions(df_pred, config):
    write_cfg = config.get_influx_config("write")

    required = {"Time", "Link_ID", "Side", "Predicted_Temperature"}
    missing = required - set(df_pred.columns)
    if missing:
        backend_logger.error(f"Chybí sloupce pro zápis do InfluxDB: {missing}")
        return False

    try:
        with InfluxDBClient(url=write_cfg["url"], token=write_cfg["token"], org=write_cfg["org"]) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            points = []
            for _, row in df_pred.iterrows():
                try:
                    t_utc = row["Time"].to_pydatetime()
                    cml_id = str(row["Link_ID"]).strip()
                    side = str(row["Side"]).strip()
                    value = float(row["Predicted_Temperature"])

                    p = (
                        Point(write_cfg["measurement"])
                        .tag(write_cfg["tag_cml_id"], cml_id)
                        .tag(write_cfg["tag_side"], side)
                        .field(write_cfg["field_temperature"], value)
                        .time(t_utc)
                    )
                    points.append(p)
                except Exception as row_err:
                    backend_logger.warning(f"Přeskakuji řádek kvůli chybě konverze: {row_err}")

            if points:
                write_api.write(bucket=write_cfg["bucket"], record=points)
                backend_logger.info(
                    f"Do InfluxDB zapsáno {len(points)} bodů "
                    f"(bucket='{write_cfg['bucket']}', measurement='{write_cfg['measurement']}')."
                )
                return True

            backend_logger.warning("Nebyl připraven žádný bod k zápisu.")
            return False

    except Exception as e:
        backend_logger.error(f"Chyba při zápisu do InfluxDB: {e}")
        return False
