from sqlalchemy import text
from telcotemp.data_sources.base import MetadataProvider
import time


class CMLMetadataProvider(MetadataProvider):
    """Fetch CML link metadata from MySQL."""

    def __init__(self, engine, logger):
        self.engine = engine
        self.logger = logger
        self._cache = {}

    def fetch_metadata(self, ips):
        """Fetch metadata for CML IPs."""
        t0 = time.perf_counter()

        # Check cache
        cached = [ip for ip in ips if ip in self._cache]
        missing = [ip for ip in ips if ip not in self._cache]

        self.logger.debug(
            f"Metadata fetch: {len(cached)} cached, {len(missing)} to fetch"
        )

        if missing:
            placeholders = ",".join([f"'{ip}'" for ip in missing])

            try:
                with self.engine.connect() as conn:
                    result = conn.execute(self._query_with_measurement_mapping(placeholders))

                    for row in result:
                        self._cache[row.ip] = {
                            "link_id": row.link_id,
                            "technology": row.technology,
                            "side": row.side,
                            "azimuth": row.azimuth,
                            "altitude": row.altitude,
                            "lat": row.lat,
                            "lon": row.lon,
                        }

                    self.logger.debug(
                        f"Fetched metadata for {len(self._cache) - len(cached)} new IPs"
                    )

            except Exception as e:
                self.logger.warning(
                    "Mapped technology metadata query failed, falling back to raw technology values: %s",
                    e,
                )
                try:
                    with self.engine.connect() as conn:
                        result = conn.execute(self._fallback_query(placeholders))

                        for row in result:
                            self._cache[row.ip] = {
                                "link_id": row.link_id,
                                "technology": row.technology,
                                "side": row.side,
                                "azimuth": row.azimuth,
                                "altitude": row.altitude,
                                "lat": row.lat,
                                "lon": row.lon,
                            }
                except Exception as fallback_error:
                    self.logger.error(f"Error fetching metadata: {fallback_error}")

        elapsed = time.perf_counter() - t0
        self.logger.debug(f"Metadata fetch completed in {elapsed:.3f}s")

        return self._cache

    def _query_with_measurement_mapping(self, placeholders):
        return text(
            f"""
                SELECT
                    l.ID AS link_id,
                    COALESCE(
                        m.measurement,
                        st.influx_measurement,
                        CAST(l.technology AS CHAR)
                    ) AS technology,
                    x.ip AS ip,
                    x.side AS side,
                    x.azimuth AS azimuth,
                    s.altitude AS altitude,
                    s.X_coordinate AS lon,
                    s.Y_coordinate AS lat
                FROM cml_metadata.links l
                JOIN (
                    SELECT ID, IP_address_A AS ip, 'A' AS side, site_A AS site_id, azimuth_A AS azimuth
                    FROM cml_metadata.links
                    UNION ALL
                    SELECT ID, IP_address_B AS ip, 'B' AS side, site_B AS site_id, azimuth_B AS azimuth
                    FROM cml_metadata.links
                ) x ON x.ID = l.ID
                JOIN cml_metadata.sites s ON s.id = x.site_id
                LEFT JOIN cml_metadata.technologies t ON t.ID = l.technology
                LEFT JOIN cml_metadata.technologies_influx_mapping m ON t.influx_mapping_ID = m.ID
                LEFT JOIN cml_metadata.show_technologies st ON st.ID = l.technology
                WHERE x.ip IN ({placeholders})
            """
        )

    def _fallback_query(self, placeholders):
        return text(
            f"""
                SELECT
                    l.ID AS link_id,
                    l.technology AS technology,
                    x.ip AS ip,
                    x.side AS side,
                    x.azimuth AS azimuth,
                    s.altitude AS altitude,
                    s.X_coordinate AS lon,
                    s.Y_coordinate AS lat
                FROM cml_metadata.links l
                JOIN (
                    SELECT ID, IP_address_A AS ip, 'A' AS side, site_A AS site_id, azimuth_A AS azimuth
                    FROM cml_metadata.links
                    UNION ALL
                    SELECT ID, IP_address_B AS ip, 'B' AS side, site_B AS site_id, azimuth_B AS azimuth
                    FROM cml_metadata.links
                ) x ON x.ID = l.ID
                JOIN cml_metadata.sites s ON s.id = x.site_id
                WHERE x.ip IN ({placeholders})
            """
        )
