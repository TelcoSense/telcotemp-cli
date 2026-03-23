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
        self._fixed_scale_cache = {}

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
            if self.scale_mode == "dynamic":
                cmap, vmin, vmax, colormap_info = self._get_dynamic_scale(grid_z)
            elif self.scale_mode == "static_cz_adaptive":
                cmap, vmin, vmax, colormap_info = self._get_static_cz_adaptive_scale(
                    grid_z
                )
            elif self.scale_mode == "static_cz":
                cmap, vmin, vmax, colormap_info = self._get_static_cz_scale()
            else:
                cmap, vmin, vmax, colormap_info = self._get_static_scale()

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
        return self._build_fixed_scale("static", self._static_colormap())

    def _get_static_cz_scale(self):
        """
        Returns a denser Czech-focused fixed temperature color scale.

        :return: (colormap, vmin, vmax, colormap_info)
        """
        return self._build_fixed_scale(
            "static_cz",
            self._static_cz_colormap(),
            under_color="#5f007a",
            over_color="#7f0000",
        )

    def _get_static_cz_adaptive_scale(self, grid_z):
        """
        Uses the static Czech palette, but adapts the displayed range to the
        current grid so narrow-range days keep more contrast.
        """
        cmap, hard_min, hard_max, colormap_info = self._get_static_cz_scale()
        values = np.asarray(grid_z, dtype=float)
        values = values[np.isfinite(values)]
        if values.size == 0:
            return cmap, hard_min, hard_max, colormap_info

        low = float(np.nanpercentile(values, 5))
        high = float(np.nanpercentile(values, 95))
        median = float(np.nanmedian(values))

        margin_c = 1.0
        min_span_c = 10.0

        vmin = float(np.floor(low - margin_c))
        vmax = float(np.ceil(high + margin_c))

        if (vmax - vmin) < min_span_c:
            half_span = min_span_c / 2.0
            vmin = float(np.floor(median - half_span))
            vmax = float(np.ceil(median + half_span))

        if vmin < hard_min:
            shift = hard_min - vmin
            vmin = float(hard_min)
            vmax = float(min(hard_max, vmax + shift))

        if vmax > hard_max:
            shift = vmax - hard_max
            vmax = float(hard_max)
            vmin = float(max(hard_min, vmin - shift))

        if vmin >= vmax:
            vmin = float(max(hard_min, median - 5.0))
            vmax = float(min(hard_max, median + 5.0))

        return cmap, vmin, vmax, colormap_info

    def map_plotting(
        self, grid_x, grid_y, grid_z, czech_rep, image_name, show_boundary=False
    ):
        """Alias for backward compatibility."""
        return self.plot(grid_x, grid_y, grid_z, czech_rep, image_name, show_boundary)

    def _default_colormap(self):
        """
        Returns the default color scale.
        """
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

    def _static_cz_colormap(self):
        """
        Returns a denser absolute scale tailored to Czech temperatures.
        """
        return [
            (-20, "#7000d8"),
            (-18, "#6200de"),
            (-16, "#5300e4"),
            (-14, "#3f10e8"),
            (-12, "#2a28eb"),
            (-10, "#1540ee"),
            (-9, "#0d50ee"),
            (-8, "#0660ed"),
            (-7, "#0170eb"),
            (-6, "#007fe8"),
            (-5, "#038ee3"),
            (-4, "#069cdc"),
            (-3, "#0caad4"),
            (-2, "#15b8cb"),
            (-1, "#1fc5c1"),
            (0, "#22cfe1"),
            (1, "#18d7df"),
            (2, "#10dddb"),
            (3, "#0ce3d4"),
            (4, "#0ce8ca"),
            (5, "#0eebbd"),
            (6, "#12edaf"),
            (7, "#18ef9c"),
            (8, "#24ef82"),
            (9, "#37ec61"),
            (10, "#52e52b"),
            (11, "#74de1d"),
            (12, "#97db1b"),
            (13, "#badc1b"),
            (14, "#d8df1a"),
            (15, "#ecd81a"),
            (16, "#f5c619"),
            (17, "#fcb019"),
            (18, "#ff871b"),
            (19, "#ff8019"),
            (20, "#ff681c"),
            (21, "#ff5021"),
            (22, "#ff3a27"),
            (23, "#ff1d2e"),
            (24, "#ff0e36"),
            (25, "#fa0241"),
            (26, "#e8004b"),
            (27, "#d60055"),
            (28, "#c3005d"),
            (29, "#b00065"),
            (30, "#9d006a"),
            (31, "#89006d"),
            (32, "#75006f"),
            (33, "#62006f"),
            (34, "#4f006c"),
            (35, "#3d0067"),
            (40, "#21003b"),
        ]

    def _build_fixed_scale(
        self, scale_name, scale_points, under_color=None, over_color=None
    ):
        cache_key = (scale_name, under_color, over_color)
        if cache_key not in self._fixed_scale_cache:
            temps, _ = zip(*scale_points)
            vmin = temps[0]
            vmax = temps[-1]
            normalized_scale = [
                ((t - vmin) / (vmax - vmin), color) for t, color in scale_points
            ]
            cmap = mcolors.LinearSegmentedColormap.from_list(
                f"{scale_name}_colormap", normalized_scale, N=256
            )
            if under_color is not None:
                cmap.set_under(under_color)
            if over_color is not None:
                cmap.set_over(over_color)
            colormap_info = [
                {"level": temp, "color": color} for temp, color in scale_points
            ]
            if under_color is not None:
                colormap_info.append({"level": "<min", "color": under_color})
            if over_color is not None:
                colormap_info.append({"level": ">max", "color": over_color})
            self._fixed_scale_cache[cache_key] = (cmap, vmin, vmax, colormap_info)
        return self._fixed_scale_cache[cache_key]

