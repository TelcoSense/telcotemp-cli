import json
import os
import logging

backend_logger = logging.getLogger("backend_logger")


def save_color_scale(color_scale_info, image_name, config, logger):
    paths = config.get_paths()
    save_dir = paths["color_scale_dir"]
    output_dir = save_dir
    os.makedirs(output_dir, exist_ok=True)
    color_scale_path = os.path.join(output_dir, f"{image_name}_color_scale.json")

    with open(color_scale_path, "w") as f:
        json.dump(color_scale_info, f, indent=4)

    logger.info(f"Color scale settings saved to {color_scale_path}")
