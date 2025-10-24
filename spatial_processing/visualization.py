import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import os


class MapVisualizer:
    """
    Class for visualizing temperature maps.
    """

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.vis = config.get_visualization()
        self.paths = config.get_paths()

    def map_plotting(
        self, grid_x, grid_y, grid_z, czech_rep, image_name, show_boundary=False
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
            n_levels = self.vis["n_levels"]
            colormap = self.vis["colormap"] or self._default_colormap()
            median_offset = self.vis["median_offset"]

            self.logger.info("map_plotting: %s", image_name)

            cmap = mcolors.LinearSegmentedColormap.from_list(
                "custom_colormap", colormap, N=n_levels
            )
            temperature_median = np.nanmedian(grid_z) - median_offset
            vmin = int(temperature_median) - 7
            vmax = int(temperature_median) + 7

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

            save_dir = self.paths["images_dir"]
            os.makedirs(save_dir, exist_ok=True)
            base_name, ext = os.path.splitext(image_name)
            save_path = os.path.join(save_dir, f"{base_name}_{vmin}_{vmax}{ext}")

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
                "colors": [
                    {"level": level, "color": color} for level, color in colormap
                ],
            }
        except Exception as e:
            self.logger.exception("Exception in map_plotting: %s", e)
            raise

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
