#!/usr/bin/env python3
#
# Osprey script
# https://github.com/Smithsonian/Osprey_worker
#
# Regenerate JPG/Deep Zoom previews for the main files in one folder that is
# already registered in the dashboard, without re-running the full
# validation pipeline. Reuses settings.regenerate_previews: existing preview
# artifacts are left alone unless that setting is True, in which case they
# are all rebuilt (same behavior as the main pipeline).
#
############################################
# Import modules
############################################
import argparse
import importlib.util
import os
import signal
import sys
from multiprocessing import Pool, Semaphore
from pathlib import Path

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
    parser = argparse.ArgumentParser(
        description="Osprey Worker — regenerate previews for one folder"
    )
    parser.add_argument("--version", action="version", version=f"osprey-worker {ver}")
    default_settings = os.environ.get("OSPREY_SETTINGS")
    parser.add_argument(
        "--settings",
        default=default_settings,
        help="Path to settings.py (default: $OSPREY_SETTINGS or ./settings.py)",
    )
    parser.add_argument(
        "folder",
        help="Dashboard folder name, or a path (relative to settings.project_datastorage, or absolute)",
    )
    return parser.parse_args()


_args = _parse_args()
settings = _load_settings(_args.settings)

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
from worker.utils import compress_log, check_disk_space
from worker.api import send_request
from worker.previews import generate_previews, set_zoom_semaphore

import logutil

############################################
# Logging
############################################
log_folder = "logs"
logger = logutil.setup_main_logger(f"{settings.project_alias}_regen", None, log_folder)
logger.info(f"osprey-worker {ver} — regenerate_previews")


############################################
# Helpers
############################################
def _resolve_folder(project_datastorage, folder_arg):
    """Accept a dashboard folder name or a path; return (folder_path, folder_name)."""
    folder_path = folder_arg if os.path.isabs(folder_arg) else os.path.join(project_datastorage, folder_arg)
    return folder_path, os.path.basename(os.path.normpath(folder_path))


def _find_folder_id(project_info, folder_name):
    """Look up the folder_id of an already-registered dashboard folder by name."""
    for folder in project_info.get("folders", []):
        if folder["folder"] == folder_name:
            return folder["folder_id"]
    return None


def _collect_main_files(folder_path, main_suffix):
    """Recursively collect files under folder_path matching settings.main_files."""
    matches = []
    for root, _dirs, names in os.walk(folder_path):
        for name in names:
            if Path(name).suffix == main_suffix:
                matches.append(os.path.join(root, name))
    return sorted(matches)


def _init_worker(logfile_folder, zoom_semaphore):
    """Pool initializer: per-process log file + Deep Zoom concurrency limit."""
    logutil.setup_worker_logger(logfile_folder)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    set_zoom_semaphore(zoom_semaphore)


def _regen_one(task):
    """Pool/serial worker: regenerate previews for one file."""
    file_id, folder_id, file_path, jpg_previews, tar_previews, transcription = task
    worker_logger = logutil.get_worker_logger()
    ok = generate_previews(
        file_id, folder_id, file_path, worker_logger, jpg_previews,
        tar_previews=tar_previews, transcription=transcription,
    )
    return file_path, file_id, ok


def _log_result(i, total, result, ok_count, fail_count):
    file_path, file_id, ok = result
    logger.info(f"[{i}/{total}] {'OK' if ok else 'FAILED'}: {file_path} (file_id={file_id})")
    return (ok_count + 1, fail_count) if ok else (ok_count, fail_count + 1)


############################################
# Main
############################################
def main():
    """
    Regenerate previews for every main file in one dashboard folder.
    """
    folder_path, folder_name = _resolve_folder(settings.project_datastorage, _args.folder)
    if not os.path.isdir(folder_path):
        logger.error(f"Folder not found: {folder_path}")
        return False
    if not check_disk_space(settings, logger):
        return False

    default_payload = {"api_key": settings.api_key}
    project_info = send_request(
        f"{settings.api_url}/projects/{settings.project_alias}", default_payload, logger, log_res=False
    )
    if project_info is None:
        return False

    folder_id = _find_folder_id(project_info, folder_name)
    if folder_id is None:
        logger.error(f"Folder '{folder_name}' is not registered in the dashboard yet")
        return False

    folder_info = send_request(
        f"{settings.api_url}/folders/{folder_id}", default_payload, logger, log_res=False
    )
    if folder_info is None:
        return False
    stem_to_id = {f["file_name"]: f["file_id"] for f in folder_info["files"]}

    main_files = _collect_main_files(folder_path, settings.main_files)
    logger.info(f"Found {len(main_files)} '{settings.main_files}' file(s) in {folder_path}")
    if not main_files:
        logger.info("Nothing to do")
        return True

    transcription = project_info["transcription"]
    tar_previews = getattr(settings, "tar_previews", False)

    tasks = []
    for file_path in main_files:
        file_id = stem_to_id.get(Path(file_path).stem)
        if file_id is None:
            logger.error(f"Skipping {file_path}: no matching file registered for folder {folder_id}")
            continue
        tasks.append((file_id, folder_id, file_path, settings.jpg_previews, tar_previews, transcription))

    if not tasks:
        logger.error("No files could be matched to dashboard records")
        return False

    ok_count = 0
    fail_count = 0
    num_workers = getattr(settings, "num_workers", 1)

    if num_workers > 1:
        zoom_limit = getattr(settings, "zoom_workers", 4)
        zoom_sem = Semaphore(zoom_limit) if zoom_limit > 0 else None
        logger.info(f"Regenerating {len(tasks)} file(s) on {num_workers} workers")
        with Pool(num_workers, initializer=_init_worker, initargs=(log_folder, zoom_sem)) as pool:
            for i, result in enumerate(pool.imap_unordered(_regen_one, tasks), start=1):
                ok_count, fail_count = _log_result(i, len(tasks), result, ok_count, fail_count)
    else:
        logger.info(f"Regenerating {len(tasks)} file(s) sequentially")
        for i, task in enumerate(tasks, start=1):
            ok_count, fail_count = _log_result(i, len(tasks), _regen_one(task), ok_count, fail_count)

    logger.info(f"Done: {ok_count} succeeded, {fail_count} failed (of {len(tasks)})")
    return fail_count == 0


############################################
# Main loop
############################################
if __name__ == "__main__":
    try:
        success = main()
    except KeyboardInterrupt:
        print("\nInterrupted, compressing logs...")
        success = False
    finally:
        compress_log()
    sys.exit(0 if success else 1)
