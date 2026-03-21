import geopandas as gpd
from shapely.geometry import Polygon, Point
from shapely import contains_xy
import numpy as np
import json
import rasterio


class GeographicalProcessing:
    """Geographical processing operations (identical for both modes)."""
    
    def json_to_geodataframe(self, json_data):
        """Convert JSON data to GeoDataFrame."""
        geometries = []

        for feature in json_data["features"]:
            poly = Polygon(feature["geometry"]["coordinates"][0])
            geometries.append(poly)

        gdf = gpd.GeoDataFrame(geometry=geometries, crs="EPSG:4326")
        return gdf

    def create_mask(self, czech_rep, grid_x, grid_y):
        """Create mask for grid points inside Czech Republic."""
        geom = (
            czech_rep.union_all()
            if hasattr(czech_rep, "union_all")
            else czech_rep.unary_union
        )
        return contains_xy(geom, grid_x, grid_y)

    def create_mask_slow(self, czech_rep, grid_x, grid_y):
        """Fallback implementation kept for debugging/reference."""
        mask = np.zeros_like(grid_x, dtype=bool)
        for i in range(grid_x.shape[0]):
            for j in range(grid_x.shape[1]):
                point = Point(grid_x[i, j], grid_y[i, j])
                mask[i, j] = czech_rep.contains(point).any()
        return mask

    def load_country_data(self, country_file_path):
        """Load country boundary data from JSON."""
        with open(country_file_path, "r", encoding="utf-8") as file:
            return json.load(file)

    def load_elevation_data(self, tif_path):
        """
        Load elevation data from GeoTIFF.
        Returns: elevation_data (2D array), transform (Affine), crs (CRS)
        """
        with rasterio.open(tif_path) as src:
            elevation_data = src.read(1)
            nodata = src.nodata
            if nodata is not None:
                elevation_data = np.where(elevation_data == nodata, np.nan, elevation_data)

            transform_matrix = src.transform  # Affine
            crs = src.crs  # rasterio.crs.CRS

        return elevation_data, transform_matrix, crs
