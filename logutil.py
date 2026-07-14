import logging
import os
import time


def setup_main_logger(project_alias, worker_set=None, log_folder="logs"):
    os.makedirs(log_folder, exist_ok=True)
    current_time = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    if worker_set is not None:
        logfile = f"{log_folder}/{project_alias}_w{worker_set}_{current_time}.log"
    else:
        logfile = f"{log_folder}/{project_alias}_{current_time}.log"

    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(logging.Formatter(
        '%(levelname)s | %(asctime)s | %(filename)s:%(lineno)s | %(funcName)s | %(message)s',
        datefmt='%Y-%b-%d %H:%M:%S'))

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        '%(levelname)s | %(filename)s:%(lineno)s | %(funcName)s | %(message)s'))
    console_handler.setLevel(logging.INFO)

    logger = logging.getLogger("osprey")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.info(f"Logging to {logfile}")
    return logger


def setup_worker_logger(log_folder):
    os.makedirs(log_folder, exist_ok=True)
    handler = logging.FileHandler(f"{log_folder}/workers.log")
    handler.setFormatter(logging.Formatter(
        '%(levelname)s | %(asctime)s | PID %(process)d | %(filename)s:%(lineno)s | %(message)s',
        datefmt='%Y-%b-%d %H:%M:%S'))
    logger = logging.getLogger("osprey.worker")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False


def get_worker_logger():
    """
    Logger for per-file worker tasks.
    Falls back to the main osprey logger when pool workers are not initialized
    (e.g. num_workers=1 serial mode in the parent process).
    """
    worker = logging.getLogger("osprey.worker")
    if worker.handlers:
        return worker
    return logging.getLogger("osprey")
