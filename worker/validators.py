import os
import subprocess
import tempfile
import time
import xmltodict
from pathlib import Path
from PIL import Image

import logging

from worker.api import send_request


def jhove_validate(file_path, jhove_binary, tmp_folder, max_retries=3):
    suffix = Path(file_path).suffix.lower()
    jhove_module = {
        ".tif": "TIFF-hul",
        ".tiff": "TIFF-hul",
        ".jpg": "JPEG-hul",
        ".jpeg": "JPEG-hul",
        ".jp2": "JPEG2000-hul",
        ".png": "PNG-gdm",
    }.get(suffix, "BYTESTREAM")

    last_error = None

    for attempt in range(max_retries):
        tmp = tempfile.NamedTemporaryFile(suffix='.xml', dir=tmp_folder, prefix='jhove_', delete=False)
        xml_file = tmp.name
        tmp.close()

        try:
            try:
                p = subprocess.run(
                    [jhove_binary, "-h", "xml", "-o", xml_file, "-m", jhove_module, file_path],
                    capture_output=True, timeout=30
                )
            except subprocess.TimeoutExpired:
                last_error = f"JHOVE timed out after 30s on {file_path}"
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return 1, last_error

            jhove_results = p.stdout.decode('latin-1')

            if p.returncode != 0 and not os.path.isfile(xml_file):
                last_error = f"JHOVE failed (rc={p.returncode}): {p.stderr.decode('latin-1')}"
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return 1, last_error

            if not os.path.isfile(xml_file):
                last_error = f"JHOVE produced no output XML for {file_path}"
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return 1, last_error

            with open(xml_file) as fd:
                doc = xmltodict.parse(fd.read())

            rep_info = doc.get('jhove', {}).get('repInfo', {})
            file_status = rep_info.get('status', 'Unknown')

            if file_status == "Well-Formed and valid":
                return 0, f"{file_status}; {jhove_results}"

            check_results = 1
            messages = rep_info.get('messages', {}).get('message')
            if messages is None:
                return check_results, f"{file_status} (no messages); {jhove_results}"

            if isinstance(messages, dict):
                messages = [messages]

            status_parts = []
            for msg in messages:
                text = msg.get('#text', '')
                if text.startswith("WhiteBalance value out of range"):
                    check_results = 0
                    status_parts.append(text)
                elif text.startswith("Unknown TIFF IFD tag"):
                    check_results = 0
                    status_parts.append(text)
                else:
                    status_parts.append(text)

            file_status = "; ".join(status_parts) if status_parts else file_status
            return check_results, f"{file_status}; {jhove_results}"

        except Exception as e:
            last_error = f"Unexpected error in jhove_validate: {e}"
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return 1, last_error

        finally:
            if os.path.isfile(xml_file):
                os.unlink(xml_file)


def magick_validate(filename, magick_binary=None, magick_thread_limit=None, paranoid=False):
    """
    Validate the file with ImageMagick identify.
    """
    cmd = [magick_binary if magick_binary is not None else 'identify']
    if paranoid:
        cmd.extend(['-verbose', '-regard-warnings', filename])
    else:
        cmd.extend(['-verbose', filename])

    env = os.environ.copy()
    if magick_thread_limit is not None:
        env['MAGICK_THREAD_LIMIT'] = str(magick_thread_limit)

    p = subprocess.run(cmd, capture_output=True, env=env)
    check_results = 0 if p.returncode == 0 else 1
    check_info = (p.stdout + p.stderr).decode('latin-1')
    return check_results, check_info


def check_sequence(filename, project_files, sequence, sequence_split, project_files_by_stem=None):
    if not sequence:
        return (None, 1, "sequence setting is empty")

    if project_files_by_stem is None:
        if project_files:
            project_files_by_stem = {f['file_name']: f for f in project_files}
        else:
            project_files_by_stem = {}

    filename_stem = Path(filename).stem
    file_rec = project_files_by_stem.get(filename_stem)
    file_id = file_rec['file_id'] if file_rec else None
    if file_id is None:
        check_results = 1
        check_info = f"file_id not found for {filename}"
        return (file_id, check_results, check_info)
    file_suffix = filename_stem.split(sequence_split)
    file_wo_suffix = file_suffix[0:len(file_suffix) - 1]
    file_wo_suffix = '_'.join(file_wo_suffix)
    file_suffix = file_suffix[len(file_suffix) - 1]
    if file_suffix == sequence[len(sequence) - 1]:
        check_results = 0
        check_info = "File is the last one in the sequence"
        return (file_id, check_results, check_info)
    for i in range(len(sequence)):
        if file_suffix == sequence[i]:
            next_in_seq = sequence[i + 1]
            next_filename_stem = "{}{}{}".format(file_wo_suffix, sequence_split, next_in_seq)
            if project_files_by_stem.get(next_filename_stem):
                check_results = 0
                check_info = f"Next file in sequence ({next_filename_stem}) found"
                return (file_id, check_results, check_info)
    check_results = 1
    check_info = "Next file in sequence was not found"
    return (file_id, check_results, check_info)


def sequence_validate(filename, folder_id, project_files, logger=None):
    """
    Validate sequence and post result to the API (standalone / backward-compatible).
    The main pipeline uses check_sequence() inside process_image_worker instead.
    """
    import settings
    if logger is None:
        logger = logging.getLogger("osprey.worker")
    _, check_results, check_info = check_sequence(
        filename, project_files, settings.sequence, settings.sequence_split
    )
    payload = {
        'type': 'file',
        'property': 'filechecks',
        'folder_id': folder_id,
        'file_id': _sequence_file_id(filename, project_files),
        'api_key': settings.api_key,
        'file_check': 'sequence',
        'value': check_results,
        'check_info': check_info,
    }
    query_results = send_request(
        f"{settings.api_url}/update/{settings.project_alias}", payload, logger
    )
    if query_results is None or not query_results.get("result"):
        return False
    return check_results == 0


def _sequence_file_id(filename, project_files):
    filename_stem = Path(filename).stem
    for file in project_files:
        if file['file_name'] == filename_stem:
            return file['file_id']
    return None


def tif_compression(file_path: str = None, img=None) -> tuple:
    """
    Check if a TIFF file uses lossless compression.
    Returns (0, compression_name) on success, (1, error_message) on failure.
    Pass either file_path or an open Pillow image.
    """
    if img is None:
        try:
            img = Image.open(file_path)
        except Exception as e:
            return 1, f"File opening error: {file_path} - {e}"

    try:
        comp = img.info['compression']
    except KeyError:
        return 1, "Missing compression field in TIFF metadata"

    LOSSLESS_COMPRESSIONS = {
        'tiff_lzw',
        'tiff_adobe_deflate',
        'tiff_deflate',
        'packbits'
    }

    if comp in LOSSLESS_COMPRESSIONS:
        return 0, comp
    return 1, f"Unsupported (non-lossless) compression: {comp}"


def tifpages(file_path: str = None, img=None) -> tuple:
    """
    Check if TIF has multiple pages using Pillow.
    Pass either file_path or an open Pillow image.
    """
    if img is None:
        try:
            img = Image.open(file_path)
        except Exception as e:
            return 1, f"File opening error: {file_path} - {e}"

    try:
        no_pages = img.n_frames
    except AttributeError:
        return 1, "TIFF n_frames attribute not available (not a multi-frame TIFF or corrupted)"

    if no_pages == 1:
        return 0, no_pages
    return 1, f"More than 1 page found in the TIF, found {no_pages}"
