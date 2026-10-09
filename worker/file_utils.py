import os
import hashlib
import subprocess
from pathlib import Path

_HASH_CHUNK = 1024 * 1024


def get_file_exif(filename, exiftool_binary, logger=None):
    """
    Extract the EXIF info from the RAW file.
    Returns a UTF-8 string suitable for API POST, or False on failure.
    """
    try:
        p = subprocess.run(
            [exiftool_binary, '-j', '-L', '-a', '-U', '-u', '-D', '-G1', filename],
            capture_output=True, timeout=60
        )
    except subprocess.TimeoutExpired:
        if logger is not None:
            logger.error(f"exiftool timed out on {filename}")
        return False
    if p.returncode != 0:
        if logger is not None:
            err = p.stderr.decode('latin-1', errors='replace')
            logger.error(f"exiftool failed on {filename}: {err}")
        return False
    return p.stdout.decode('utf-8', errors='replace')


def hash_file(filepath):
    """Return hex MD5 digest for a file, or None on error."""
    # usedforsecurity=False: integrity check only; required on FIPS-enabled RHEL
    md5_hash = hashlib.md5(usedforsecurity=False)
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(_HASH_CHUNK), b""):
                md5_hash.update(chunk)
    except OSError:
        return None
    return md5_hash.hexdigest()


def get_filemd5(filepath, logger):
    """
    Get MD5 hash of a file
    """
    if not os.path.isfile(filepath):
        return False
    digest = hash_file(filepath)
    if digest is None:
        return False
    return digest


def build_raw_index(raw_files):
    """
    Build a dict mapping file stem to raw file path.
    """
    return {Path(r).stem: r for r in raw_files}


def _load_expected_md5(md5_files):
    """
    Load sidecar MD5 files into {basename: md5_hex}.
    Returns (expected, total_entries). Raises ValueError on duplicate basenames,
    OSError if a sidecar can't be read.
    """
    expected = {}
    total_entries = 0
    for md5f in md5_files:
        with open(md5f, 'r', encoding='utf-8', errors='replace') as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split(None, 1)
                if len(parts) < 2:
                    continue
                md5_hex, file_ref = parts[0], parts[1]
                # md5sum -b / Windows tools prefix the name with '*' (binary mode)
                name = Path(file_ref.lstrip('*')).name
                if name in expected:
                    raise ValueError(f"Duplicate MD5 entry for filename: {name}")
                expected[name] = md5_hex.upper()
                total_entries += 1
    return expected, total_entries


def _md5_check_one(filepath, expected_md5):
    """Worker task: verify one file and return (path, ok, digest)."""
    if expected_md5 is None:
        return filepath, 1, None
    digest = hash_file(filepath)
    if digest is None:
        return filepath, 1, None
    # expected_md5 is already uppercased by _load_expected_md5
    ok = 0 if digest.upper() == expected_md5 else 1
    return filepath, ok, digest


def _md5_check_star(args):
    """Unpack (filepath, expected_md5) for imap_unordered; top-level so it pickles."""
    return _md5_check_one(*args)


def validate_md5(md5_files, files, pool=None, logger=None):
    """
    Validate files against MD5 sidecars.
    Returns (check_code, message, digests) where digests maps filepath -> hex md5.
    """
    try:
        expected_by_name, total_entries = _load_expected_md5(md5_files)
    except ValueError as e:
        return 1, str(e), None
    except OSError as e:
        return 1, f"Cannot read MD5 file: {e}", None

    if len(files) != total_entries:
        return 1, (
            f"File count mismatch: {len(files)} files vs {total_entries} MD5 entries"
        ), None

    expected_for_files = []
    for filepath in files:
        name = Path(filepath).name
        if name not in expected_by_name:
            return 1, f"MD5 entry missing for {name}", None
        expected_for_files.append(expected_by_name[name])

    total = len(files)

    def _log_md5_progress(done, filepath):
        # Log first, last, and every 10th completed file
        if logger is None or total == 0:
            return
        if done == 1 or done == total or done % 10 == 0:
            msg = f"MD5 check {done}/{total}: {Path(filepath).name}"
            logger.info(msg)
            print(msg)

    tasks = list(zip(files, expected_for_files))
    if pool is None:
        results_iter = map(_md5_check_star, tasks)
    else:
        # chunksize=1: hand out one file at a time so uneven file sizes balance
        # across workers; results arrive in completion order
        results_iter = pool.imap_unordered(_md5_check_star, tasks, chunksize=1)

    results = []
    for i, res in enumerate(results_iter, start=1):
        results.append(res)
        _log_md5_progress(i, res[0])

    bad = sum(r[1] for r in results)
    digests = {r[0]: r[2] for r in results if r[2] is not None}
    if bad == 0:
        return 0, "Valid MD5", digests
    return 1, f"{bad} file(s) don't match MD5 file", digests
