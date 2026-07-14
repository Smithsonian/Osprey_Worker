"""Batch API request flushing with bounded concurrency."""

from concurrent.futures import ThreadPoolExecutor, as_completed

from worker.api import send_request


def flush_api_queue(api_queue, settings, logger, api_workers=8):
    """
    POST queued API requests with a bounded thread pool.
    Returns False if any request fails.
    """
    if not api_queue:
        return True

    url_update = f"{settings.api_url}/update/{settings.project_alias}"
    url_new = f"{settings.api_url}/new/{settings.project_alias}"

    def _send(item):
        url = url_new if item.get('kind') == 'new' else url_update
        return send_request(url, item['payload'], logger, log_res=item.get('log_res', True))

    workers = min(max(1, api_workers), len(api_queue))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_send, item) for item in api_queue]
        for future in as_completed(futures):
            if future.result() is None:
                return False
    return True
