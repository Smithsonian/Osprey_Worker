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
import logging
import os
import time
import requests
import numpy as np
import json
import locale
import sys
import subprocess
import tarfile

# Import settings from settings.py file
import settings

# Import helper functions
from functions import *

ver = "2.9.0"

# Pass an argument in the CLI 'debug'
if len(sys.argv) == 4:
    run_debug = sys.argv[1]
    worker_set = int(sys.argv[2])
    no_sets = int(sys.argv[3])
elif len(sys.argv) == 2:
    run_debug = sys.argv[1]
    worker_set = None
    no_sets = None
else:
    run_debug = False
    worker_set = None
    no_sets = None


############################################
# Logging
############################################
log_folder = "logs"

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Logging
current_time = time.strftime("%Y%m%d_%H%M%S", time.localtime())
if worker_set != None:
    logfile = '{}/{}_w{}_{}.log'.format(log_folder, settings.project_alias, worker_set, current_time)
else:
    logfile = '{}/{}_{}.log'.format(log_folder, settings.project_alias, current_time)
logging.basicConfig(filename=logfile, filemode='a', level=logging.DEBUG,
                    format='%(levelname)s | %(asctime)s | %(filename)s:%(lineno)s | %(message)s',
                    datefmt='%Y-%b-%d %H:%M:%S')
logger = logging.getLogger("osprey")
logging.info("osprey version {}".format(ver))

# Set locale for number format
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')


############################################
# Check requirements
############################################
if check_requirements(settings.jhove) is False:
    logger.error("JHOVE was not found")
    sys.exit(1)


if check_requirements(settings.exiftool) is False:
    logger.error("exiftool was not found")
    sys.exit(1)


if check_requirements(settings.magick) is False:
    logger.error("imagemagick was not found")
    sys.exit(1)


############################################
# Main
############################################
def main():
    """
    Main function to validate images in digitization projects.
    """
    if not os.path.isdir(settings.project_datastorage):
        logger.error("Path not found: {}".format(settings.project_datastorage))
        sys.exit(1)
    r = requests.get('{}/api/'.format(settings.api_url))
    if r.status_code != 200:
        # Something went wrong
        query_results = r.text.encode('utf-8')
        logger.error("API Returned Error: {}".format(query_results))
        sys.exit(1)
    system_info = json.loads(r.text.encode('utf-8'))
    if system_info['sys_ver'] != ver:
        logger.error("API version ({}) does not match this script ({})".format(system_info['sys_ver'], ver))
        sys.exit(1)
    default_payload = {'api_key': settings.api_key}
    r = requests.post('{}/api/projects/{}'.format(settings.api_url, settings.project_alias), data=default_payload)
    if r.status_code != 200:
        # Something went wrong
        query_results = r.text.encode('utf-8')
        logger.error("API Returned Error: {}".format(query_results))
        sys.exit(1)
    project_info = json.loads(r.text.encode('utf-8'))
    # Reset folders under verification and other pending tasks
    if worker_set == 0 or worker_set is None:
        logger.info("Clearing project in database")
        payload = {'type': 'startup',
                   'property': 'startup',
                   'api_key': settings.api_key,
                   'value': True
                   }
        r = requests.post('{}/api/update/{}'.format(settings.api_url, settings.project_alias), data=payload)
        if r.status_code != 200:
            # Something went wrong
            query_results = r.text.encode('utf-8')
            logger.error("API Returned Error: {}".format(query_results))
            sys.exit(1)
    else:
        # Wait 20 seconds for first
        logger.info("Waiting for project to clear in database")
        time.sleep(20)
    # Generate list of folders in the path
    folders = []
    for entry in os.scandir(settings.project_datastorage):
        if entry.is_dir() and entry.path != settings.project_datastorage:
            print(entry)
            folders.append(entry.path)
        else:
            logger.error("Extraneous files in: {}".format(entry.path))
            sys.exit(1)
    # No folders found
    if len(folders) == 0:
        logger.info("No folders found in: {}".format(settings.project_datastorage))
        return True
    # Check each folder
    logger.info("project_info: {}".format(project_info))
    for folder in folders:
        working_on = "Working on folder: {}".format(folder)
        logger.info(working_on)
        print(working_on)
        res = run_checks_folder_p(project_info, folder, log_folder, logger)
        if res is False:
            logger.error("Folder {} returned error".format(folder))
        # Tar the files
        fol_data = "{}/folder{}".format(settings.jpg_previews, res)
        if os.path.isdir(fol_data):
            os.chdir("{}/folder{}".format(settings.jpg_previews, res))
            for entry in os.scandir("."):
                if entry.is_dir() and entry.name[-6:] == "_files":
                    try:
                        print("Tar of previews of {}".format(entry.name))
                        tar = tarfile.open("{}.tar".format(entry.name), "w")
                        tar.add(entry.name)
                        tar.close()
                        shutil.rmtree(entry.name)
                    except:
                        logger.error("Error tar for {}".format(res))
    logger.info("Script completed on {}".format(time.strftime("%Y%m%d_%H%M%S", time.localtime())))
    return True


############################################
# Main loop
############################################
if __name__ == "__main__":
    main()
    compress_log()

