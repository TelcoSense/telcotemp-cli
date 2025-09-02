import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, bindparam
import time
backend_logger = logging.getLogger('backend_logger')


class DatabaseOperations:
    def __init__(self, engine):
        self.engine = engine
        self.Session = sessionmaker(bind=self.engine)
        self._ip_meta_cache = {}

    def get_metadata(self, df):
        t0 = time.perf_counter()

        sites, azimuths, latitudes, longitudes, links, technologies, sides = [], [], [], [], [], [], []
        to_drop = []
        devices = 0

        ips_series = df["IP"].astype(str).str.strip()
        unique_ips = sorted({ip for ip in ips_series if ip})

        backend_logger.debug("get_metadata: %d řádků, %d unikátních IP.", len(df), len(unique_ips))

        cached = {ip: self._ip_meta_cache[ip] for ip in unique_ips if ip in self._ip_meta_cache}
        missing = [ip for ip in unique_ips if ip not in cached]

        fetched = {}
        if missing:
            try:
                with self.Session() as session:
                    stmt = text("""
                        SELECT
                            l.ID            AS link_id,
                            l.technology    AS technology,
                            x.ip            AS ip,
                            x.side          AS side,
                            x.site_id       AS site_id,
                            x.azimuth       AS azimuth,
                            s.X_coordinate  AS lon,
                            s.Y_coordinate  AS lat
                        FROM cml_metadata.links l
                        JOIN (
                            SELECT ID, IP_address_A AS ip, 'A' AS side, site_A AS site_id, azimuth_A AS azimuth FROM cml_metadata.links
                            UNION ALL
                            SELECT ID, IP_address_B AS ip, 'B' AS side, site_B AS site_id, azimuth_B AS azimuth FROM cml_metadata.links
                        ) x ON x.ID = l.ID
                        JOIN cml_metadata.sites s ON s.id = x.site_id
                        WHERE x.ip IN :ips
                    """).bindparams(bindparam("ips", expanding=True))

                    result = session.execute(stmt, {"ips": missing}).all()

                    for r in result:
                        m = r._mapping
                        rec = {
                            "link_id": m["link_id"],
                            "technology": m["technology"],
                            "ip": m["ip"],
                            "side": m["side"],
                            "site_id": m["site_id"],
                            "azimuth": m["azimuth"],
                            "lon": m["lon"],
                            "lat": m["lat"],
                        }
                        fetched[rec["ip"]] = rec
                        self._ip_meta_cache[rec["ip"]] = rec
            except Exception as e:
                backend_logger.error(f"Error during bulk metadata fetch: {e}")

        lookup = {**cached, **fetched}

        for idx, ip in enumerate(ips_series):
            if not ip:
                backend_logger.warning("No link result found for IP: <empty>")
                to_drop.append(idx)
                continue

            meta = lookup.get(ip)
            if meta is None:
                backend_logger.debug(f"No link result found for IP: {ip}")
                to_drop.append(idx)
                continue

            azimuths.append(meta["azimuth"])
            sites.append(meta["site_id"])
            links.append(meta["link_id"])
            latitudes.append(meta["lat"])
            longitudes.append(meta["lon"])
            technologies.append(meta["technology"])
            sides.append(meta["side"])
            devices += 1

        if to_drop:
            df.drop(index=to_drop, inplace=True)
            df.reset_index(drop=True, inplace=True)

        backend_logger.info(f"Completed get_metadata method for {devices} devices.")
        backend_logger.debug(
            "get_metadata: cache_hit=%d, fetched=%d, elapsed=%.3fs",
            len(cached), len(fetched), time.perf_counter() - t0
        )

        return latitudes, longitudes, azimuths, links, technologies, sides

