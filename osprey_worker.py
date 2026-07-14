#!/usr/bin/env python3
#
# Osprey script
# https://github.com/Smithsonian/Osprey_worker
#
# Validate images in Digitization Projects
#
############################################
# Import modules
############################################
import argparse
import importlib.util
import locale
import os
import sys
import time

from worker.version import __version__ as ver


def _load_settings(settings_path):
    """Load settings module from file path into sys.modules['settings']."""
    if not settings_path:
        settings_path = os.path.join(os.getcwd(), "settings.py")
    if not os.path.isfile(settings_path):
        print(f"Fatal: settings file not found: {settings_path}", file=sys.stderr)
        sys.exit(1)
    spec = importlib.util.spec_from_file_location("settings", settings_path)
    if spec is None or spec.loader is None:
        print(f"Fatal: cannot load settings from {settings_path}", file=sys.stderr)
        sys.exit(1)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    sys.modules["settings"] = mod
    return mod


def _parse_args():
    parser = argparse.ArgumentParser(description="Osprey Worker — validate digitization deliverables")
    parser.add_argument("--version", action="version", version=f"osprey-worker {ver}")
    default_settings = os.environ.get("OSPREY_SETTINGS")
    parser.add_argument(
        "--settings",
        default=default_settings,
        help="Path to settings.py (default: $OSPREY_SETTINGS or ./settings.py)",
    )
    parser.add_argument(
        "--worker-set",
        type=int,
        default=None,
        help="Worker index; 0 or omitted clears dashboard pending state at startup",
    )
    return parser.parse_args()


_args = _parse_args()
settings = _load_settings(_args.settings)
worker_set = _args.worker_set

# Validate critical settings at startup
_required = {
    "project_alias": settings.project_alias,
    "api_url": settings.api_url,
    "api_key": settings.api_key,
    "project_datastorage": settings.project_datastorage,
    "jpg_previews": settings.jpg_previews,
}
for _name, _value in _required.items():
    if not _value:
        print(f"Fatal: settings.{_name} is not configured", file=sys.stderr)
        sys.exit(1)

# Import helper functions (after settings is in sys.modules)
from worker.utils import check_requirements, compress_log, check_disk_space
from worker.pipeline import run_checks_folder_p
from worker.api import send_get, send_request

import logutil

############################################
# Logging
############################################
log_folder = "logs"

logger = logutil.setup_main_logger(settings.project_alias, worker_set, log_folder)
logger.info(f"osprey version {ver}")

# Set locale for number format
try:
    locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
except locale.Error:
    locale.setlocale(locale.LC_ALL, "")


############################################
# Check requirements
############################################
if check_requirements(settings.jhove, "JHOVE", logger) is None:
    sys.exit(1)

if check_requirements(settings.exiftool, "exiftool", logger) is None:
    sys.exit(1)

if check_requirements(settings.identify_path, "ImageMagick", logger) is None:
    sys.exit(1)

if check_requirements("zip", "zip", logger) is None:
    sys.exit(1)


############################################
# Main
############################################
def main():
    """
    Main function to validate images in digitization projects.
    """
    if not os.path.isdir(settings.project_datastorage):
        logger.error(f"Path not found: {settings.project_datastorage}")
        sys.exit(1)
    if not check_disk_space(settings, logger):
        sys.exit(1)
    system_info = send_get(f"{settings.api_url}", logger)
    if system_info is None:
        sys.exit(1)
    if system_info["sys_ver"] != ver:
        logger.error(f"API version ({system_info['sys_ver']}) does not match this script ({ver})")
        sys.exit(1)
    default_payload = {"api_key": settings.api_key}
    project_info = send_request(
        f"{settings.api_url}/projects/{settings.project_alias}", default_payload, logger, log_res=False
    )
    if project_info is None:
        sys.exit(1)
    if "sequence" in project_info.get("project_checks", []) and not settings.sequence:
        logger.error("sequence check enabled but settings.sequence is empty")
        sys.exit(1)
    # Reset folders under verification and other pending tasks
    if worker_set == 0 or worker_set is None:
        logger.info("Clearing project in database")
        payload = {
            "type": "startup",
            "property": "startup",
            "api_key": settings.api_key,
            "value": True,
        }
        r = send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger)
        if r is None:
            sys.exit(1)
    else:
        # Wait 20 seconds for first
        logger.info("Waiting for project to clear in database")
        time.sleep(20)
    # Generate list of folders in the path (vendor deliverables only — no loose files at root)
    folders = []
    for entry in os.scandir(settings.project_datastorage):
        if entry.is_dir() and entry.path != settings.project_datastorage:
            folders.append(entry.path)
        elif not entry.is_dir():
            logger.error(f"Extraneous files in: {entry.path}")
            sys.exit(1)
    # No folders found
    if len(folders) == 0:
        logger.info(f"No folders found in: {settings.project_datastorage}")
        return True
    # Check each folder
    for folder in folders:
        working_on = f"Working on folder: {folder}"
        logger.info(working_on)
        print(working_on)
        res = run_checks_folder_p(settings, project_info, folder, log_folder, logger)
        if res is False:
            logger.error(f"Folder {folder} returned error")
            # sys.exit(1)
    logger.info("Script completed on {}".format(time.strftime("%Y%m%d_%H%M%S", time.localtime())))
    return True


############################################
# Main loop
############################################
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted, compressing logs...")
    finally:
        compress_log()
