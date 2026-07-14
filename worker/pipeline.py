"""Image validation pipeline orchestration."""

import os
import locale
import logging
import signal
import time
from contextlib import contextmanager
from collections import Counter
from datetime import datetime
from pathlib import Path
from multiprocessing import Pool, Semaphore

import settings
import logutil
from worker.api import send_request, update_folder_stats, update_folder_preview_type
from worker.api_flush import flush_api_queue
from worker.validators import (
    jhove_validate, magick_validate, check_sequence,
    tif_compression, tifpages
)
from worker.file_utils import get_file_exif, hash_file, validate_md5, build_raw_index
from worker.previews import (
    generate_previews, resolve_preview_base, resolve_preview_type, set_zoom_semaphore,
)
from worker.utils import folder_date


# ---------------------------------------------------------------------------
# Pool helpers
# ---------------------------------------------------------------------------


def _magick_thread_limit(settings, num_workers):
    if num_workers > 1:
        cpu = os.cpu_count() or 4
        return max(1, cpu // num_workers)
    limit = getattr(settings, 'magick_limit', None)
    return max(1, limit) if limit else 1


def _apply_thread_limits(settings, num_workers):
    os.environ['MAGICK_THREAD_LIMIT'] = str(_magick_thread_limit(settings, num_workers))
    os.environ['OMP_NUM_THREADS'] = '1'
    os.environ['OPENBLAS_NUM_THREADS'] = '1'


def _init_worker(logfile_folder, num_workers, zoom_semaphore=None):
    """Pool initializer: logging, SIGINT, thread limits, and Deep Zoom concurrency."""
    logutil.setup_worker_logger(logfile_folder)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    _apply_thread_limits(settings, num_workers)
    set_zoom_semaphore(zoom_semaphore)


def _log_progress(logger, message):
    """Phase/progress line on console and main log."""
    logger.info(message)
    print(message)


def _log_file_progress(logger, completed, total, result):
    """Parent-process progress line (console + main log)."""
    name = Path(result.get('filename', '')).name or str(result.get('filename', '?'))
    _log_progress(logger, f"Completed {completed}/{total}: {name}")


@contextmanager
def _phase_timer(logger, settings, phase_name):
    """Log elapsed time for a pipeline phase when settings.profile_timing is True."""
    if not getattr(settings, 'profile_timing', False):
        yield
        return
    start = time.monotonic()
    yield
    elapsed = time.monotonic() - start
    _log_progress(logger, f"phase={phase_name} elapsed={elapsed:.2f}s")


def _run_pool_imap_failfast(pool, tasks, logger, settings):
    """
    Run worker tasks with imap_unordered; flush API queue in parent after each result.
    Terminates the pool early on fatal worker failure or API flush failure.
    """
    api_workers = getattr(settings, 'api_workers', 8)
    total = len(tasks)
    results = []
    aborted = False
    try:
        for result in pool.imap_unordered(process_image_worker, tasks, chunksize=1):
            results.append(result)
            _log_file_progress(logger, len(results), total, result)
            if not flush_api_queue(result['api_queue'], settings, logger, api_workers):
                logger.error(f"API flush failed for {result.get('filename')}")
                aborted = True
                pool.terminate()
                break
            if not result['ok'] and result.get('fatal'):
                logger.error(f"Fatal worker error for {result.get('filename')}: {result.get('error')}")
                aborted = True
                pool.terminate()
                break
            if result.get('sequence_ok') is False:
                logger.error(f"Sequence check failed for {result.get('filename')}")
                aborted = True
                pool.terminate()
                break
    except Exception as e:
        logger.error(f"Pool processing error: {e}")
        pool.terminate()
        aborted = True
    return not aborted and len(results) == len(tasks)


# ---------------------------------------------------------------------------
# API queue builders (used by workers; flushed in parent)
# ---------------------------------------------------------------------------


def _q_update(api_queue, payload, log_res=True):
    api_queue.append({'kind': 'update', 'payload': payload, 'log_res': log_res})


def _q_new(api_queue, payload, log_res=True):
    api_queue.append({'kind': 'new', 'payload': payload, 'log_res': log_res})


def _q_filecheck(api_queue, file_id, folder_id, file_check, check_results, check_info,
                 sanitize_path=True):
    if sanitize_path and check_info:
        check_info = check_info.replace(settings.project_datastorage, "")
    _q_update(api_queue, {
        'type': 'file',
        'property': 'filechecks',
        'folder_id': folder_id,
        'file_id': file_id,
        'api_key': settings.api_key,
        'file_check': file_check,
        'value': check_results,
        'check_info': check_info,
    })


# ---------------------------------------------------------------------------
# Helpers extracted from run_checks_folder_p
# ---------------------------------------------------------------------------


def _find_or_create_folder(settings, project_info, folder_name, folder_path, logger):
    transcription = project_info['transcription']
    folder_id = None
    delivered_to_dams = None
    if len(project_info['folders']) > 0:
        for folder in project_info['folders']:
            if folder['folder'] == folder_name:
                folder_id = folder['folder_id']
                delivered_to_dams = folder['delivered_to_dams']
                break
    if folder_id is None:
        try:
            folder_date_str = folder_date(folder_name)
        except ValueError as e:
            logger.error(str(e))
            return None
        payload = {
            'type': 'folder',
            'api_key': settings.api_key,
            'folder': folder_name,
            'folder_path': folder_path,
            'folder_date': folder_date_str,
            'project_id': project_info['project_id']
        }
        if settings.uuid_ids:
            payload['id_type'] = 'uuid'
        r = send_request(f"{settings.api_url}/new/{settings.project_alias}", payload, logger)
        if r is None:
            return None
        if transcription == 1:
            folder_id = r["result"][0]['folder_transcription_id']
        else:
            folder_id = r["result"][0]['folder_id']
        delivered_to_dams = 9
    return folder_id, delivered_to_dams, transcription


def _walk_folder(folder_path, logger=None):
    """Walk a folder tree using scandir with error handling for network filesystems.

    Returns (file_paths, skipped_count). Any skipped path should fail the folder.
    """
    files = []
    skipped = 0
    try:
        for entry in os.scandir(folder_path):
            try:
                if entry.is_dir(follow_symlinks=False):
                    sub_files, sub_skipped = _walk_folder(entry.path, logger)
                    files.extend(sub_files)
                    skipped += sub_skipped
                elif entry.is_file():
                    files.append(entry.path)
            except OSError as e:
                if logger is not None:
                    logger.warning(f"Skipping unreadable path: {entry.path} ({e})")
                skipped += 1
                continue
    except (OSError, PermissionError) as e:
        if logger is not None:
            logger.warning(f"Cannot read directory: {folder_path} ({e})")
        skipped += 1
    return files, skipped


def _check_folder_skip_conditions(settings, folder_id, delivered_to_dams, folder_info, logger):
    if delivered_to_dams == 0 or delivered_to_dams == 1:
        logger.info(f"Folder ready for or delivered to DAMS, skipping")
        return True
    if folder_info is None:
        return True
    if folder_info['qc_status'] != "QC Pending":
        logger.info(f"Folder QC has been completed, skipping")
        return True
    if folder_info['status'] == 0 and folder_info['file_errors'] == 0:
        logger.info(f"Folder has been completed, skipping")
        return True
    return False


def _tag_folder_checking(settings, folder_id, logger):
    payload = {'type': 'folder',
               'folder_id': folder_id,
               'api_key': settings.api_key,
               'property': 'checking_folder',
               'value': 1
               }
    r = send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger)
    return r is not None


def _classify_and_validate_files(files, project_checks, folder_info, folder_id, settings, logger):
    image_files = []
    image_main_files = []
    md5_allowed_files = []
    if 'raw_pair' in project_checks:
        allowed_files = [settings.md5_file, settings.main_files, settings.raw_files]
        allowed_image_files = [settings.main_files, settings.raw_files]
        md5_file_suffixes = [settings.main_files, settings.raw_files]
    else:
        allowed_files = [settings.md5_file, settings.main_files]
        allowed_image_files = [settings.main_files]
        md5_file_suffixes = [settings.main_files]
    if settings.data_files is not None:
        if settings.data_files not in allowed_files:
            allowed_files.append(settings.data_files)
        if settings.data_files not in md5_file_suffixes:
            md5_file_suffixes.append(settings.data_files)

    for file in files:
        if Path(file).suffix not in allowed_files:
            file_suffix = Path(file).suffix
            payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key,
                       'property': 'status1',
                       'value': f'Extraneous files: {file} ({file_suffix} not in {allowed_files})'}
            r = send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger)
            if r is None:
                return None
            return None
        if Path(file).suffix in md5_file_suffixes:
            md5_allowed_files.append(file)
        if Path(file).suffix in allowed_image_files:
            image_files.append(file)
        if Path(file).suffix == settings.main_files:
            image_main_files.append(file)

    main_stem_counts = Counter(Path(f).stem for f in image_main_files)

    api_files = folder_info['files']
    api_workers = getattr(settings, 'api_workers', 8)
    delete_queue = []
    stale_stems = set()

    for file in api_files:
        total = main_stem_counts.get(file['file_name'], 0)
        dup_name = file['file_name'] if total > 1 else None
        if total == 0:
            delete_queue.append({
                'kind': 'update',
                'payload': {
                    'type': 'file',
                    'file_id': file['file_id'],
                    'api_key': settings.api_key,
                    'property': 'delete',
                    'value': True,
                },
                'log_res': False,
            })
            stale_stems.add(file['file_name'])
        elif total > 1:
            payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key,
                       'property': 'status1', 'value': f'Dupe file in folder ({dup_name})'}
            r = send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger)
            if r is None:
                return None
            update_folder_stats(settings.api_url, settings.api_key, settings.project_alias, folder_id, logger)
            return None

    if delete_queue:
        _log_progress(logger, f"Removing {len(delete_queue)} stale file record(s) from API...")
        if not flush_api_queue(delete_queue, settings, logger, api_workers):
            return None
        folder_info['files'] = [f for f in folder_info['files'] if f['file_name'] not in stale_stems]

    for file in files:
        if " " in file:
            payload = {'type': 'folder',
                       'folder_id': folder_id,
                       'api_key': settings.api_key,
                       'property': 'filename_spaces',
                       'value': 1
                       }
            r = send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger)
            if r is None:
                return None
            update_folder_stats(settings.api_url, settings.api_key, settings.project_alias, folder_id, logger)
            return None

    return image_files, image_main_files, md5_allowed_files


def _validate_folder_md5(settings, files, md5_allowed_files, folder_id, logger, pool=None):
    md5_files = [f for f in files if Path(f).suffix == settings.md5_file]
    if len(md5_files) == 0:
        folder_status_msg = "MD5 files missing"
        payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key,
                   'property': 'status1', 'value': folder_status_msg}
        r = send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger)
        if r is None:
            return False, None
        update_folder_stats(settings.api_url, settings.api_key, settings.project_alias, folder_id, logger)
        return False, None

    md5_check, md5_error, digests = validate_md5(
        md5_files, md5_allowed_files, pool=pool, logger=logger
    )
    md5_property = 'tif_md5_matches_ok' if md5_check == 0 else 'tif_md5_matches_error'
    payload = {'type': 'folder',
               'folder_id': folder_id,
               'api_key': settings.api_key,
               'property': md5_property,
               'value': md5_error
               }
    r = send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger)
    if r is None:
        return False, None
    if md5_check != 0:
        payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key,
                   'property': 'status1', 'value': md5_error}
        r = send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger)
        if r is None:
            return False, None
        return False, digests
    return True, digests


def _verify_raw_pair_counts(settings, files, image_main_files, project_checks, folder_id, logger):
    if 'raw_pair' not in project_checks:
        return []
    raw_files = [file for file in files if Path(file).suffix == settings.raw_files]
    if len(image_main_files) != len(raw_files):
        folder_status_msg = f"No. of files do not match (main: {len(image_main_files)}, raws: {len(raw_files)})"
        payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key,
                   'property': 'status1', 'value': folder_status_msg}
        r = send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger)
        if r is None:
            return None
        update_folder_stats(settings.api_url, settings.api_key, settings.project_alias, folder_id, logger)
        return None
    return raw_files


def _find_or_create_image_file(settings, filename, folder_id, folder_info, logger, stem_to_id=None):
    filename_stem = Path(filename).stem
    if stem_to_id is not None and filename_stem in stem_to_id:
        return stem_to_id[filename_stem]
    for file in folder_info['files']:
        if file['file_name'] == filename_stem:
            return file['file_id']

    filename_suffix = Path(filename).suffix[1:]
    main_file_path = filename
    file_timestamp_str = datetime.fromtimestamp(os.path.getmtime(main_file_path)).strftime('%Y-%m-%d %H:%M:%S')
    payload = {
        'api_key': settings.api_key,
        'type': "file",
        'folder_id': folder_id,
        'filename': filename_stem,
        'timestamp': file_timestamp_str,
        'filetype': filename_suffix.lower(),
    }
    if settings.uuid_ids:
        payload['id_type'] = 'uuid'
    file_info = send_request(f"{settings.api_url}/new/{settings.project_alias}", payload, logger)
    if file_info is None:
        return None
    file_id = file_info['result'][0]['file_id']

    payload = {
        'api_key': settings.api_key,
        'type': "filesize",
        'file_id': file_id,
        'filetype': filename_suffix.lower(),
        'filesize': os.path.getsize(main_file_path)
    }
    if send_request(f"{settings.api_url}/new/{settings.project_alias}", payload, logger) is None:
        return None
    folder_info['files'].append({
        'file_id': file_id,
        'file_name': filename_stem,
    })
    if stem_to_id is not None:
        stem_to_id[filename_stem] = file_id
    return file_id


def _check_unique_constraints(settings, file_id, folder_id, project_checks, logger):
    if 'unique_file' in project_checks:
        payload = {'type': 'file',
                   'property': 'unique',
                   'folder_id': folder_id,
                   'file_id': file_id,
                   'api_key': settings.api_key,
                   'file_check': 'unique_file',
                   'value': True,
                   'check_info': True
                   }
        if send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger) is None:
            return False
    if 'unique_other' in project_checks:
        payload = {'type': 'file',
                   'property': 'unique_other',
                   'folder_id': folder_id,
                   'file_id': file_id,
                   'api_key': settings.api_key,
                   'file_check': 'unique_other',
                   'value': True,
                   'check_info': True
                   }
        if send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger) is None:
            return False
    return True


def _register_folder_files(settings, image_main_files, folder_id, folder_info, project_checks, logger):
    """Register all main files in the API serially before parallel workers start."""
    registry = {}
    stem_to_id = {f['file_name']: f['file_id'] for f in folder_info['files']}
    total = len(image_main_files)
    for i, filename in enumerate(image_main_files, start=1):
        if total > 1 and (i == 1 or i == total or i % 10 == 0):
            _log_progress(logger, f"Registering file {i}/{total}: {Path(filename).name}")
        file_id = _find_or_create_image_file(
            settings, filename, folder_id, folder_info, logger, stem_to_id=stem_to_id
        )
        if file_id is None:
            return None
        if not _check_unique_constraints(settings, file_id, folder_id, project_checks, logger):
            return None
        registry[filename] = file_id
    return registry


def _process_raw_pair_check(settings, file_name, raw_index, file_id, folder_id, logger, api_queue,
                            digest_cache=None):
    raw_file = raw_index.get(Path(file_name).stem)
    if raw_file is None:
        check_results = 1
        check_info = f"Raw file not found for {file_name} ({file_id})"
        _q_update(api_queue, {
            'type': 'file',
            'property': 'filemd5_missing_raw',
            'file_id': file_id,
            'api_key': settings.api_key,
            'filetype': "",
            'value': check_info
        })
        return check_results, check_info

    check_results = 0
    check_info = f"Raw file {Path(raw_file).name} found for {file_name} ({file_id})."
    rawfile_suffix = Path(raw_file).suffix[1:]
    check_results1, check_info1 = jhove_validate(raw_file, settings.jhove, settings.tmp_folder)
    check_results2, check_info2 = magick_validate(raw_file, settings.identify_path, magick_thread_limit=None)
    if check_results1 == 1:
        res1 = f"JHOVE could not validate: {check_info1}"
    else:
        res1 = f"JHOVE validated the file: {check_info1}"
        check_results1 = 0
    if check_results2 == 1:
        if rawfile_suffix == "eip":
            check_results2 = 0
            res2 = ""
        else:
            res2 = f"Imagemagick could not validate: {check_info2}"
    else:
        res2 = f"Imagemagick validated the file: {check_info2}"
        check_results2 = 0
    if (check_results1 + check_results2) > 0:
        check_results = 1
    file_md5 = digest_cache.get(raw_file) if digest_cache else None
    if file_md5 is None:
        file_md5 = hash_file(raw_file)
    if file_md5 is None:
        return None
    raw_filetype = Path(raw_file).suffix[1:]
    _q_update(api_queue, {
        'type': 'file',
        'property': 'filemd5',
        'file_id': file_id,
        'api_key': settings.api_key,
        'filetype': raw_filetype.lower(),
        'value': file_md5
    })
    _q_new(api_queue, {
        'api_key': settings.api_key,
        'type': "filesize",
        'file_id': file_id,
        'filetype': raw_filetype.lower(),
        'filesize': os.path.getsize(raw_file)
    })
    check_info = f"{check_info}; {res1}; {res2}"
    return check_results, check_info


def _raw_digest_cache(raw_index, md5_digests):
    """Subset of MD5 digests for raw-pair files only."""
    if not raw_index or not md5_digests:
        return {}
    return {path: md5_digests[path] for path in raw_index.values() if path in md5_digests}


def _build_worker_tasks(image_main_files, folder_id, file_registry, project_checks,
                        md5_digests, run_sequence, transcription, raw_index,
                        project_files_by_stem, raw_digest_cache):
    tasks = []
    for filename in image_main_files:
        md5_digest = md5_digests.get(filename) if md5_digests else None
        tasks.append((
            filename,
            folder_id,
            file_registry[filename],
            project_checks,
            md5_digest,
            run_sequence,
            transcription,
            raw_index,
            project_files_by_stem,
            raw_digest_cache,
        ))
    return tasks


def _process_folder_images(settings, image_main_files, folder_id, folder_info, raw_files,
                           project_checks, md5_digests, logfile_folder, logger, pool=None,
                           transcription=0):
    run_sequence = 'sequence' in project_checks
    if run_sequence and not settings.sequence:
        logger.error("sequence check enabled but settings.sequence is empty")
        return False

    with _phase_timer(logger, settings, 'register'):
        file_registry = _register_folder_files(
            settings, image_main_files, folder_id, folder_info, project_checks, logger
        )
    if file_registry is None:
        return False

    raw_index = build_raw_index(raw_files) if raw_files else {}
    project_files_by_stem = None
    if run_sequence:
        default_payload = {'api_key': settings.api_key}
        project_files = send_request(
            f"{settings.api_url}/projects/{settings.project_alias}/files",
            default_payload, logger, log_res=False
        )
        if project_files is None:
            return False
        project_files_by_stem = {f['file_name']: f for f in project_files}

    raw_digest_cache = _raw_digest_cache(raw_index, md5_digests)
    tasks = _build_worker_tasks(
        image_main_files, folder_id, file_registry, project_checks,
        md5_digests, run_sequence, transcription, raw_index, project_files_by_stem,
        raw_digest_cache
    )
    no_tasks = len(tasks)
    api_workers = getattr(settings, 'api_workers', 8)

    if pool is None:
        _apply_thread_limits(settings, 1)
        logger.info(f"Started run of {locale.format_string('%d', no_tasks, grouping=True)} tasks for folder")
        print(f"Started run of {locale.format_string('%d', no_tasks, grouping=True)} tasks for folder")
        for i, task in enumerate(tasks, start=1):
            name = Path(task[0]).name
            _log_progress(logger, f"Processing {i}/{no_tasks}: {name}")
            result = process_image_worker(task)
            _log_file_progress(logger, i, no_tasks, result)
            if not flush_api_queue(result['api_queue'], settings, logger, api_workers):
                return False
            if not result['ok'] and result.get('fatal'):
                return False
            if result.get('sequence_ok') is False:
                return False
        return True

    logger.info(
        f"Started parallel run of {locale.format_string('%d', no_tasks, grouping=True)} tasks "
        f"on {settings.num_workers} workers"
    )
    return _run_pool_imap_failfast(pool, tasks, logger, settings)


# ---------------------------------------------------------------------------
# run_checks_folder_p
# ---------------------------------------------------------------------------


def run_checks_folder_p(settings, project_info, folder_path, logfile_folder, logger):
    """
    Process a folder in parallel
    """
    logger.info(f"Processing folder: {folder_path}")
    folder_name = os.path.basename(folder_path)

    result = _find_or_create_folder(settings, project_info, folder_name, folder_path, logger)
    if result is None:
        return False
    folder_id, delivered_to_dams, transcription = result

    default_payload = {'api_key': settings.api_key}
    folder_info = send_request(f"{settings.api_url}/folders/{folder_id}", default_payload, logger, log_res=False)
    if folder_info is None:
        return False
    project_checks = project_info['project_checks']

    if _check_folder_skip_conditions(settings, folder_id, delivered_to_dams, folder_info, logger):
        return folder_id

    if not _tag_folder_checking(settings, folder_id, logger):
        return False

    try:
        os.listdir(folder_path)
    except OSError as e:
        logger.error(f"Cannot list folder {folder_path}: {e}")
        return False
    logger.info(f"Walking folder: {folder_path}")
    files, walk_skipped = _walk_folder(folder_path, logger)
    logger.info(f"Found {len(files)} files in {folder_path}")
    if walk_skipped > 0:
        folder_status_msg = f"Unreadable paths during folder walk ({walk_skipped} skipped)"
        logger.error(folder_status_msg)
        payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key,
                   'property': 'status1', 'value': folder_status_msg}
        r = send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger)
        if r is None:
            return False
        update_folder_stats(settings.api_url, settings.api_key, settings.project_alias, folder_id, logger)
        return False

    _log_progress(logger, f"Classifying {len(files)} files...")
    with _phase_timer(logger, settings, 'classify'):
        classified = _classify_and_validate_files(files, project_checks, folder_info, folder_id, settings, logger)
    if classified is None:
        return False
    image_files, image_main_files, md5_allowed_files = classified
    _log_progress(
        logger,
        f"Classified {len(image_main_files)} main files, {len(md5_allowed_files)} for MD5 check "
        f"({len(folder_info['files'])} files in API)"
    )

    md5_digests = {}
    pool = None
    use_pool = settings.num_workers > 1

    if use_pool:
        zoom_limit = getattr(settings, 'zoom_workers', 4)
        zoom_sem = Semaphore(zoom_limit) if zoom_limit > 0 else None
        pool = Pool(settings.num_workers, initializer=_init_worker,
                    initargs=(logfile_folder, settings.num_workers, zoom_sem))

    try:
        if settings.md5_required:
            _log_progress(logger, f"Verifying MD5 for {len(md5_allowed_files)} files...")
            with _phase_timer(logger, settings, 'md5'):
                md5_ok, md5_digests = _validate_folder_md5(
                    settings, files, md5_allowed_files, folder_id, logger, pool=pool
                )
            if not md5_ok:
                return False
            if md5_digests is None:
                md5_digests = {}
            _log_progress(logger, "MD5 verification complete")

        raw_files = _verify_raw_pair_counts(settings, files, image_main_files, project_checks, folder_id, logger)
        if raw_files is None:
            return False

        if len(image_main_files) > 0:
            preview_file_path = resolve_preview_base(
                folder_id, settings.jpg_previews, transcription
            )
            if not os.path.exists(preview_file_path):
                os.makedirs(preview_file_path)

        with _phase_timer(logger, settings, 'process_images'):
            if not _process_folder_images(
                settings, image_main_files, folder_id, folder_info, raw_files,
                project_checks, md5_digests, logfile_folder, logger, pool=pool,
                transcription=transcription
            ):
                return False
    finally:
        if pool is not None:
            pool.close()
            pool.join()

    logger.info(f"Folder count verification {folder_id}")
    folder_info = send_request(f"{settings.api_url}/folders/{folder_id}", default_payload, logger, log_res=False)
    if folder_info is None:
        return False
    no_files_api = len(folder_info['files'])
    no_files_main = len(image_main_files)
    logger.info(f"Folder numbers match: (folder_id:{folder_id}) {no_files_main}/{no_files_api}")
    if no_files_api != no_files_main:
        logger.error(f"Files in system ({no_files_main}) do not match files in API ({no_files_api})")
        payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key,
                   'property': 'status1', 'value': "System error"}
        r = send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger)
        if r is None or not r.get("result"):
            return False
    else:
        logger.info(f"Files in system ({no_files_main}) match files in API ({no_files_api}) (folder_id:{folder_id})")
        payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key,
                   'property': 'status0', 'value': ""}
        r = send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger)
        if r is None:
            return False

    update_folder_stats(settings.api_url, settings.api_key, settings.project_alias, folder_id, logger)

    preview_type = resolve_preview_type(getattr(settings, 'deepzoom_previews', False))
    if not update_folder_preview_type(
        settings.api_url, settings.api_key, settings.project_alias,
        folder_id, preview_type, logger
    ):
        return False

    if settings.previews_local is True:
        payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key,
                   'property': 'previews', 'value': '0'}
        r = send_request(f"{settings.api_url}/update/{settings.project_alias}", payload, logger)
        if r is None:
            return False

    logger.info(f"Folder {folder_path} completed")
    return folder_id


# ---------------------------------------------------------------------------
# process_image_worker — local work only; API flushed in parent
# ---------------------------------------------------------------------------


def process_image_worker(task):
    """
    Run checks for one image file. Returns a result dict for the parent to flush.
    """
    (filename, folder_id, file_id, project_checks,
     md5_digest, run_sequence, transcription,
     raw_index, project_files_by_stem, digest_cache) = task

    logger = logutil.get_worker_logger()
    profile = getattr(settings, 'profile_timing', False)
    api_queue = []
    result = {
        'ok': True,
        'fatal': False,
        'filename': filename,
        'file_id': file_id,
        'folder_id': folder_id,
        'api_queue': api_queue,
        'sequence_ok': None,
        'error': None,
    }

    main_file_path = filename
    filename_stem = Path(filename).stem
    filename_suffix = Path(filename).suffix[1:]
    file_name = Path(filename).name

    logger.info(f"filename: {main_file_path} (file_id: {file_id})")

    from PIL import Image
    needs_tif = 'tifpages' in project_checks or 'tif_compression' in project_checks
    img = None
    try:
        if needs_tif:
            try:
                img = Image.open(main_file_path)
            except Exception as e:
                logger.error(f"File opening error: {main_file_path} - {e}")
                result['ok'] = False
                result['fatal'] = True
                result['error'] = str(e)
                return result

        t0 = time.monotonic() if profile else None
        if not generate_previews(
            file_id, folder_id, main_file_path, logger,
            settings.jpg_previews, tar_previews=getattr(settings, 'tar_previews', False),
            transcription=transcription, img=img
        ):
            result['ok'] = False
            result['fatal'] = True
            result['error'] = 'preview generation failed'
            return result
        if profile:
            logger.info(f"phase=previews file={file_name} elapsed={time.monotonic() - t0:.2f}s")

        if md5_digest is None:
            if settings.md5_required and digest_cache:
                logger.warning(f"MD5 digest missing for {file_name}; re-hashing")
            md5_digest = hash_file(main_file_path)
        if md5_digest is None:
            result['ok'] = False
            result['fatal'] = True
            result['error'] = 'MD5 computation failed'
            return result

        _q_update(api_queue, {
            'type': 'file',
            'property': 'filemd5',
            'file_id': file_id,
            'api_key': settings.api_key,
            'filetype': filename_suffix,
            'value': md5_digest
        })

        t0 = time.monotonic() if profile else None
        exif_data = get_file_exif(main_file_path, settings.exiftool, logger)
        if profile:
            logger.info(f"phase=exif file={file_name} elapsed={time.monotonic() - t0:.2f}s")
        if exif_data is False:
            result['ok'] = False
            result['fatal'] = True
            result['error'] = 'exiftool failed'
            return result

        _q_update(api_queue, {
            'type': 'file',
            'property': 'exif',
            'file_id': file_id,
            'api_key': settings.api_key,
            'filetype': filename_suffix.lower(),
            'value': exif_data
        }, log_res=False)

        logger.info(f"Running checks on file {filename_stem} ({file_id}; folder_id: {folder_id})")

        t0 = time.monotonic() if profile else None

        if 'raw_pair' in project_checks:
            pair_result = _process_raw_pair_check(
                settings, file_name, raw_index, file_id, folder_id, logger, api_queue,
                digest_cache=digest_cache
            )
            if pair_result is None:
                result['ok'] = False
                result['fatal'] = True
                result['error'] = 'raw_pair check failed'
                return result
            check_results, check_info = pair_result
            _q_filecheck(api_queue, file_id, folder_id, 'raw_pair', check_results, check_info)

        if 'jhove' in project_checks:
            check_results, check_info = jhove_validate(main_file_path, settings.jhove, settings.tmp_folder)
            _q_filecheck(api_queue, file_id, folder_id, 'jhove', check_results, check_info)

        if 'filename' in project_checks:
            _q_filecheck(api_queue, file_id, folder_id, 'filename', 0, f"Filename valid: {filename_stem}")

        if 'tifpages' in project_checks:
            check_results, check_info = tifpages(img=img)
            logger.info(f"tifpages: {file_id} {check_results} {check_info}")
            _q_filecheck(api_queue, file_id, folder_id, 'tifpages', check_results, check_info, sanitize_path=False)

        if 'magick' in project_checks:
            check_results, check_info = magick_validate(main_file_path, settings.identify_path, magick_thread_limit=None)
            _q_filecheck(api_queue, file_id, folder_id, 'magick', check_results, check_info)

        if 'tif_compression' in project_checks:
            check_results, check_info = tif_compression(img=img)
            logger.info(f"tif_compression: {file_id} {check_results} {check_info}")
            _q_filecheck(api_queue, file_id, folder_id, 'tif_compression', check_results, check_info, sanitize_path=False)

        if run_sequence and project_files_by_stem is not None:
            seq_file_id, check_results, check_info = check_sequence(
                filename, None, settings.sequence, settings.sequence_split,
                project_files_by_stem=project_files_by_stem
            )
            _q_filecheck(api_queue, file_id, folder_id, 'sequence', check_results, check_info)
            result['sequence_ok'] = check_results == 0

        if profile:
            logger.info(f"phase=checks file={file_name} elapsed={time.monotonic() - t0:.2f}s")

        return result
    finally:
        if img is not None:
            img.close()


def process_image_p(filename, folder_id, raw_files, project_checks):
    """
    Backward-compatible entry point for single-process use.
    Registers the file and runs the worker with immediate API flush.
    """
    logger = logutil.get_worker_logger()
    default_payload = {'api_key': settings.api_key}
    folder_info = send_request(
        f"{settings.api_url}/folders/{folder_id}", default_payload, logger, log_res=False
    )
    if folder_info is None:
        return False
    file_id = _find_or_create_image_file(settings, filename, folder_id, folder_info, logger)
    if file_id is None:
        return False
    if not _check_unique_constraints(settings, file_id, folder_id, project_checks, logger):
        return False

    run_sequence = 'sequence' in project_checks
    project_files = None
    if run_sequence:
        project_files = send_request(
            f"{settings.api_url}/projects/{settings.project_alias}/files",
            default_payload, logger, log_res=False
        )
        if project_files is None:
            return False

    md5_digest = hash_file(filename)
    raw_index = build_raw_index(raw_files) if raw_files else {}
    project_files_by_stem = None
    if run_sequence and project_files is not None:
        project_files_by_stem = {f['file_name']: f for f in project_files}
    task = (
        filename, folder_id, file_id, project_checks,
        md5_digest, run_sequence, 0,
        raw_index, project_files_by_stem, {filename: md5_digest} if md5_digest else {},
    )
    _apply_thread_limits(settings, 1)
    result = process_image_worker(task)
    api_workers = getattr(settings, 'api_workers', 8)
    if not flush_api_queue(result['api_queue'], settings, logger, api_workers):
        return False
    if not result['ok']:
        return False
    if result.get('sequence_ok') is False:
        return False
    return folder_id
