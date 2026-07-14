import os
import re
import subprocess
import shutil


def folder_date(folder_name: str) -> str:
    """Extract a YYYY-MM-DD date from a folder name containing YYYYMMDD."""
    match = re.search(r'(\d{4})(\d{2})(\d{2})', folder_name)
    if match:
        return "{}-{}-{}".format(match[1], match[2], match[3])
    raise ValueError(f"No YYYYMMDD date found in folder name: {folder_name}")


def check_requirements(program, name=None, logger=None):
    """
    Check if a required program is installed and executable.
    Returns the path if found, None otherwise.
    If logger is provided, logs an error with name on failure.
    """
    path = shutil.which(program)
    if path is None and logger is not None:
        logger.error(f"{name or program} was not found (checked PATH)")
    return path


def compress_log():
    """
    Compress log files
    """
    filecheck_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(f"{filecheck_dir}/logs")
    try:
        folders = []
        files = []
        for entry in os.scandir('.'):
            if entry.is_dir():
                folders.append(entry.path)
            elif entry.is_file() and entry.path.endswith('.log'):
                files.append(entry.path)
        if len(folders) == 0 and len(files) == 0:
            return None
        for folder in folders:
            subprocess.run(["zip", "-r", f"{folder}.zip", folder], check=True, timeout=300)
            shutil.rmtree(folder)
        for file in files:
            subprocess.run(["zip", f"{file}.zip", file], check=True, timeout=300)
            os.unlink(file)
    finally:
        os.chdir(filecheck_dir)
    return True


def check_disk_space(settings, logger):
    if not settings.jpg_previews:
        return True
    if settings.jpg_previews_free is None:
        return True
    disk_check = shutil.disk_usage(settings.jpg_previews)
    free_ratio = disk_check.free / disk_check.total
    if free_ratio < settings.jpg_previews_free:
        logger.error(
            f"JPG storage location is running out of space "
            f"({round(free_ratio, 4) * 100}%) - {settings.jpg_previews}"
        )
        return False
    return True
