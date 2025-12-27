from sqlalchemy import text, bindparam
from telcotemp.data_sources.base import MetadataProvider
import time


class MeteoMetadataProvider(MetadataProvider):
    """Fetch weather station metadata from MySQL."""

    def __init__(self, engine, logger):
        self.engine = engine
        self.logger = logger
        self._cache = {}

    def fetch_metadata(self, station_ids):
        """Fetch metadata for weather stations."""
        t0 = time.perf_counter()

        # Filter out None/empty IDs
        station_ids = [
            str(sid).strip() for sid in station_ids if sid and str(sid).strip()
        ]

        if not station_ids:
            self.logger.warning("No valid station IDs to fetch")
            return self._cache

        # Check cache
        cached = [sid for sid in station_ids if sid in self._cache]
        missing = [sid for sid in station_ids if sid not in self._cache]

        self.logger.debug(
            f"Metadata fetch: {len(cached)} cached, {len(missing)} to fetch"
        )

        if missing:
            try:
                # Use correct column names and alias
                query = text(
                    """
                    SELECT
                        gh_id     AS station_id,
                        X         AS lon,
                        Y         AS lat,
                        elevation AS elev
                    FROM chmi_metadata.weather_stations
                    WHERE gh_id IN :ids
                """
                ).bindparams(bindparam("ids", expanding=True))

                with self.engine.connect() as conn:
                    result = conn.execute(query, {"ids": missing})

                    for row in result:
                        sid = str(row.station_id)  # Use the alias from SELECT
                        self._cache[sid] = {
                            "lat": float(row.lat) if row.lat else None,
                            "lon": float(row.lon) if row.lon else None,
                            "elev": float(row.elev) if row.elev else None,
                        }

                    self.logger.debug(
                        f"Fetched metadata for {len(self._cache) - len(cached)} new stations"
                    )

            except Exception as e:
                self.logger.error(f"Error fetching metadata: {e}")

        elapsed = time.perf_counter() - t0
        self.logger.debug(f"Metadata fetch completed in {elapsed:.3f}s")

        return self._cache
