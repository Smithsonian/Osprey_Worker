import time
import requests


def send_get(url, logger, timeout=60, slow_threshold=10, log_res=True):
    """
    Execute GET request to API.
    Returns the parsed JSON dict on success, None on failure.
    """
    start = time.monotonic()
    try:
        r = requests.get(url, timeout=timeout)
    except requests.RequestException as e:
        logger.error(f"send_get failed: {url}|{e}")
        return None
    elapsed = time.monotonic() - start

    if r.status_code != 200:
        logger.error(f"send_get error {r.status_code}: {url}|{r.headers}|{r.text[:200]}")
        return None

    try:
        results = r.json()
    except ValueError:
        logger.error(f"send_get non-JSON: {url}|{r.status_code}|{r.text[:200]}")
        return None

    if elapsed > slow_threshold:
        logger.warning(f"send_get (slow, {elapsed:.1f}s): {url}")
    else:
        logger.info(f"send_get ({elapsed:.1f}s): {url}")
    if log_res:
        logger.info(f"send_get_res: {results}")
    return results


def send_request(url, payload, logger, log_res=True, slow_threshold=10):
    """
    Execute request to API.
    Returns the parsed JSON dict on success, None on failure.
    Logs a warning if the request exceeds slow_threshold seconds.
    """
    safe_payload = {k: (v if k != 'api_key' else '***') for k, v in payload.items()}
    start = time.monotonic()
    try:
        r = requests.post(url, data=payload, timeout=60)
    except requests.RequestException as e:
        logger.error(f"send_request failed: {url}|{safe_payload}|{e}")
        return None
    elapsed = time.monotonic() - start

    if r.status_code != 200:
        logger.error(f"send_request error {r.status_code}: {url}|{safe_payload}|{r.headers}")
        return None

    try:
        results = r.json()
    except ValueError:
        logger.error(f"send_request non-JSON: {url}|{safe_payload}|{r.status_code}|{r.text[:200]}")
        return None

    if elapsed > slow_threshold:
        logger.warning(f"send_request (slow, {elapsed:.1f}s): {url}|{safe_payload}")
    else:
        logger.info(f"send_request ({elapsed:.1f}s): {url}|{safe_payload}")
    if log_res:
        logger.info(f"send_request_res: {results}")
    return results


def update_folder_stats(api_url, api_key, project_alias, folder_id, logger):
    """
    Update the stats for the folder
    """
    payload = {'type': 'folder',
               'folder_id': folder_id,
               'api_key': api_key,
               'property': 'stats',
               'value': '0'
               }
    r = send_request(f"{api_url}/update/{project_alias}", payload, logger)
    if r is None:
        logger.error(f"update_folder_stats failed for folder {folder_id}: send_request returned None")
        return False
    else:
        return True


def update_folder_preview_type(api_url, api_key, project_alias, folder_id, preview_type, logger):
    """Set folder preview_type to 'dzi' or 'iiif'."""
    payload = {
        'type': 'folder',
        'folder_id': folder_id,
        'api_key': api_key,
        'property': 'preview_type',
        'value': preview_type,
    }
    r = send_request(f"{api_url}/update/{project_alias}", payload, logger)
    if r is None:
        logger.error(
            f"update_folder_preview_type failed for folder {folder_id}: send_request returned None"
        )
        return False
    return True
