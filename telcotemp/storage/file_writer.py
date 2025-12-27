import os
import numpy as np


class FileWriter:
    """Saves interpolation grids to files."""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        paths = config.get_paths()
        
        # Enable saving based on config for both modes
        if config.is_grid_saving_enabled():
            self.save_dir = paths.get("saved_grids_dir", "saved_grids")
            os.makedirs(self.save_dir, exist_ok=True)
        else:
            self.save_dir = None

    def save_grid(self, grid_x, grid_y, grid_z, image_name):
        """Save grid as .npz file."""
        if self.save_dir is None:
            # Saving is disabled in the config
            return

        base_name = os.path.splitext(image_name)[0]
        file_path = os.path.join(self.save_dir, f"{base_name}.npz")

        try:
            np.savez_compressed(
                file_path, grid_x=grid_x, grid_y=grid_y, grid_z=grid_z
            )
            self.logger.info(f"Saved grid to {file_path}")
        except Exception as e:
            self.logger.error(f"Error saving grid: {e}")
