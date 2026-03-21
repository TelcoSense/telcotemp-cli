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
        self._default_colormap_cache = self._default_colormap()
        self._dynamic_scale_cache = {}
        self._static_scale_cache = None

    def plot(
        self,
        grid_x,
        grid_y,
        grid_z,
        czech_rep,
        image_name,
        output_dir,
        show_boundary=False,
    ):
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
            # save_path = os.path.join(output_dir, f"{base_name}_{vmin}_{vmax}{ext}")
            save_path = os.path.join(output_dir, f"{base_name}{ext}")

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
        Returns dynamic color scale based on data median.

        :param grid_z: Temperature grid data
        :return: (colormap, vmin, vmax, colormap_info)
        """
        n_levels = self.vis["n_levels"]
        colormap = self.vis["colormap"] or self._default_colormap_cache
        median_offset = self.vis.get("median_offset", 2)
        cache_key = (tuple(tuple(item) for item in colormap), n_levels)
        if cache_key not in self._dynamic_scale_cache:
            self._dynamic_scale_cache[cache_key] = (
                mcolors.LinearSegmentedColormap.from_list(
                    "custom_colormap", colormap, N=n_levels
                ),
                [{"level": level, "color": color} for level, color in colormap],
            )
        cmap, colormap_info = self._dynamic_scale_cache[cache_key]

        temperature_median = np.nanmedian(grid_z) - median_offset
        vmin = int(temperature_median) - 13
        vmax = int(temperature_median) + 13

        return cmap, vmin, vmax, colormap_info

    def _get_static_scale(self):
        """
        Returns static ČHMÚ color scale.

        :return: (colormap, vmin, vmax, colormap_info)
        """
        if self._static_scale_cache is None:
            chmu_scale = self._static_colormap()
            temps, _ = zip(*chmu_scale)
            vmin = temps[0]
            vmax = temps[-1]

            # Normalize temperature positions to [0, 1] range
            normalized_scale = [
                ((t - vmin) / (vmax - vmin), color) for t, color in chmu_scale
            ]

            cmap = mcolors.LinearSegmentedColormap.from_list(
                "chmu_colormap", normalized_scale, N=256
            )
            colormap_info = [
                {"level": temp, "color": color} for temp, color in chmu_scale
            ]
            self._static_scale_cache = (cmap, vmin, vmax, colormap_info)

        return self._static_scale_cache

    def map_plotting(
        self, grid_x, grid_y, grid_z, czech_rep, image_name, show_boundary=False
    ):
        """Alias for backward compatibility."""
        return self.plot(grid_x, grid_y, grid_z, czech_rep, image_name, show_boundary)

    def _default_colormap(self):
        """
        Returns the default color scale.
        """
        # return [
        #     (0, "#4E00A6"),
        #     (1 / 14, "#3600D0"),
        #     (2 / 14, "#1107F4"),
        #     (3 / 14, "#0032F7"),
        #     (4 / 14, "#0467FF"),
        #     (5 / 14, "#04A3FF"),
        #     (6 / 14, "#04D27F"),
        #     (7 / 14, "#1BEC38"),
        #     (8 / 14, "#63FF00"),
        #     (9 / 14, "#F4FB0D"),
        #     (10 / 14, "#FBE316"),
        #     (11 / 14, "#F7C41B"),
        #     (12 / 14, "#FC871D"),
        #     (13 / 14, "#DB4F08"),
        #     (1, "#A00000"),
        # ]

        return [
            (0, "#a301e3"),
            (1 / 26, "#8100e8"),
            (2 / 26, "#6101e7"),
            (3 / 26, "#4001e4"),
            (4 / 26, "#0525e4"),
            (5 / 26, "#0446ea"),
            (6 / 26, "#0367e7"),
            (7 / 26, "#0788e7"),
            (8 / 26, "#07a9e8"),
            (9 / 26, "#04cbe8"),
            (10 / 26, "#08e7e3"),
            (11 / 26, "#07e9c4"),
            (12 / 26, "#04eaa2"),
            (13 / 26, "#0ae964"),
            (14 / 26, "#0ae91d"),
            (15 / 26, "#6eec0e"),
            (16 / 26, "#b0ec0c"),
            (17 / 26, "#ceec11"),
            (18 / 26, "#ebe80e"),
            (19 / 26, "#ebc90d"),
            (20 / 26, "#eca912"),
            (21 / 26, "#ed8b11"),
            (22 / 26, "#ed6b13"),
            (23 / 26, "#f04b15"),
            (24 / 26, "#f22c0f"),
            (25 / 26, "#f01438"),
            (1, "#FF0000"),
        ]

    def _static_colormap(self):
        """
        Returns static ČHMÚ color scale (temperature in °C, color in hex).
        Fixed scale from -50°C to +50°C with non-linear intervals.
        """
        return [
            (-50, "#a301e3"),
            (-30, "#8100e8"),
            (-20, "#6101e7"),
            (-15, "#4001e4"),
            (-10, "#0525e4"),
            (-8, "#0446ea"),
            (-6, "#0367e7"),
            (-4, "#0788e7"),
            (-3, "#07a9e8"),
            (-2, "#04cbe8"),
            (-1, "#08e7e3"),
            (0, "#07e9c4"),
            (1, "#04eaa2"),
            (2, "#0ae964"),
            (3, "#6eec0e"),
            (4, "#b0ec0c"),
            (5, "#ceec11"),
            (8, "#ebe80e"),
            (10, "#ebc90d"),
            (12, "#eca912"),
            (14, "#ed8b11"),
            (16, "#ed6b13"),
            (18, "#f04b15"),
            (30, "#f22c0f"),
            (35, "#f01438"),
            (50, "#FF0000"),
        ]
