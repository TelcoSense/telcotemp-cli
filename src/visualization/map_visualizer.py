import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import os
matplotlib.use("Agg")


class MapVisualizer:
    """Unified map visualization for both modes."""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.vis = config.get_visualization()
        self.paths = config.get_paths()
        self.scale_mode = self.vis.get("scale_mode", "dynamic")

    def plot(self, grid_x, grid_y, grid_z, czech_rep, image_name, output_dir, show_boundary=False):
        """
        Plots a temperature map and saves it as an image.

        :param grid_x: X coordinates of the grid.
        :param grid_y: Y coordinates of the grid.
        :param grid_z: Temperature values on the grid.
        :param czech_rep: Geodataframe containing the boundaries of the Czech Republic.
        :param image_name: Name of the output image.
        :param show_boundary: Whether to display the boundary of the Czech Republic.
        """
        try:
            self.logger.info("map_plotting: %s", image_name)

            if self.scale_mode == "static":
                cmap, vmin, vmax, colormap_info = self._get_static_scale()
            else:
                cmap, vmin, vmax, colormap_info = self._get_dynamic_scale(grid_z)

            fig, ax = plt.subplots(figsize=(8, 4), frameon=False)
            c = ax.pcolormesh(
                grid_x,
                grid_y,
                grid_z,
                cmap=cmap,
                shading="auto",
                edgecolor="none",
                vmin=vmin,
                vmax=vmax,
            )
            if show_boundary:
                czech_rep.boundary.plot(ax=ax, linewidth=1, color="black")
            ax.set_axis_off()

            os.makedirs(output_dir, exist_ok=True)
            base_name, ext = os.path.splitext(image_name)
            save_path = os.path.join(output_dir, f"{base_name}_{vmin}_{vmax}{ext}")

            plt.savefig(
                save_path,
                format="png",
                dpi=150,
                transparent=True,
                bbox_inches="tight",
                pad_inches=0,
            )
            plt.close(fig)
            self.logger.info("Plot saved: %s", save_path)

            return {
                "vmin": vmin,
                "vmax": vmax,
                "colors": colormap_info,
            }
        except Exception as e:
            self.logger.exception("Exception in map_plotting: %s", e)
            raise
    
    def _get_dynamic_scale(self, grid_z):
        """
        Returns dynamic color scale based on data median (original behavior).
        
        :param grid_z: Temperature grid data
        :return: (colormap, vmin, vmax, colormap_info)
        """
        n_levels = self.vis["n_levels"]
        colormap = self.vis["colormap"] or self._default_colormap()
        median_offset = self.vis.get("median_offset", 2)
        
        cmap = mcolors.LinearSegmentedColormap.from_list(
            "custom_colormap", colormap, N=n_levels
        )
        
        temperature_median = np.nanmedian(grid_z) - median_offset
        vmin = int(temperature_median) - 7
        vmax = int(temperature_median) + 7
        
        colormap_info = [
            {"level": level, "color": color} 
            for level, color in colormap
        ]
        
        return cmap, vmin, vmax, colormap_info

    def _get_static_scale(self):
        """
        Returns static ČHMÚ color scale.
        
        :return: (colormap, vmin, vmax, colormap_info)
        """
        chmu_scale = self._static_colormap()
        temps, colors = zip(*chmu_scale)
        vmin = temps[0]
        vmax = temps[-1]
        
        # Normalize temperature positions to [0, 1] range
        normalized_scale = [
            ((t - vmin) / (vmax - vmin), color) 
            for t, color in chmu_scale
        ]
        
        cmap = mcolors.LinearSegmentedColormap.from_list(
            "chmu_colormap", normalized_scale, N=256
        )
        
        colormap_info = [
            {"level": temp, "color": color} 
            for temp, color in chmu_scale
        ]
        
        return cmap, vmin, vmax, colormap_info

    def map_plotting(self, grid_x, grid_y, grid_z, czech_rep, image_name, show_boundary=False):
        """Alias for backward compatibility."""
        return self.plot(grid_x, grid_y, grid_z, czech_rep, image_name, show_boundary)

    def _default_colormap(self):
        """
        Returns the default color scale.
        """
        return [
            (0, "#4E00A6"),
            (1 / 14, "#3600D0"),
            (2 / 14, "#1107F4"),
            (3 / 14, "#0032F7"),
            (4 / 14, "#0467FF"),
            (5 / 14, "#04A3FF"),
            (6 / 14, "#04D27F"),
            (7 / 14, "#1BEC38"),
            (8 / 14, "#63FF00"),
            (9 / 14, "#F4FB0D"),
            (10 / 14, "#FBE316"),
            (11 / 14, "#F7C41B"),
            (12 / 14, "#FC871D"),
            (13 / 14, "#DB4F08"),
            (1, "#A00000"),
        ]
    
    def _static_colormap(self):
        """
        Returns static ČHMÚ color scale (temperature in °C, color in hex).
        Fixed scale from -50°C to +50°C with non-linear intervals.
        """
        return [
            (-50, "#8E00B8"),
            (-30, "#6A00C8"),
            (-20, "#0000FF"),
            (-15, "#0033FF"),
            (-10, "#0066FF"),
            (-8,  "#0099FF"),
            (-6,  "#00CCFF"),
            (-4,  "#00FFFF"),
            (-3,  "#66FFFF"),
            (-2,  "#99FFFF"),
            (-1,  "#CCFFFF"),
            (0,   "#66FFCC"),
            (1,   "#33FF99"),
            (2,   "#00FF66"),
            (3,   "#66FF00"),
            (4,   "#99FF00"),
            (5,   "#CCFF00"),
            (6,   "#FFFF00"),
            (8,   "#FFCC00"),
            (10,  "#FF9900"),
            (12,  "#FF6600"),
            (14,  "#FF3300"),
            (16,  "#FF0000"),
            (18,  "#FF0000"),
            (30,  "#FF0000"),
            (35,  "#FF0000"),
            (50,  "#FF0000"),
]


