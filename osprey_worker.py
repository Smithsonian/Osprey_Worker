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

# Import settings from settings.py file
import settings
# Import helper functions
from functions import *

ver = "2.8.3"

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
                    datefmt='%y-%b-%d %H:%M:%S')
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
    if worker_set != None:
        logger.info("worker_set: {}".format(worker_set))
        logger.info("no_sets: {}".format(no_sets))
        folders.sort()
        # Break for sets
        folders_sets = np.array_split(folders, no_sets)
        folders = folders_sets[worker_set].tolist()
    # No folders found
    if len(folders) == 0:
        logger.info("No folders found in: {}".format(settings.project_datastorage))
        return True
    # Check each folder
    logger.info("project_info: {}".format(project_info))
    print("No. of folders: {}".format(len(folders)))
    for folder in folders:
        logger.info("Folders: {}".format(folder))
    for folder in folders:
        run_checks_folder_p(project_info, folder, log_folder, logger)
    logger.info("Script completed on {}".format(time.strftime("%Y%m%d_%H%M%S", time.localtime())))
    return True


############################################
# Main loop
############################################
if __name__ == "__main__":
    if run_debug == 'debug':
        print("Running debug version...")
        main()
    else:
        orig_dir = os.getcwd()
        while True:
            try:
                # Check if there is a pre script to run
                if settings.pre_script is not None:
                    p = subprocess.Popen([settings.pre_script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    (out, err) = p.communicate()
                    if p.returncode != 0:
                        print("Pre-script error")
                        print(out)
                        print(err)
                        sys.exit(9)
                    else:
                        print(out)
                # Run main function
                mainval = main()
                # Return to main dir
                os.chdir(orig_dir)
                # Check if there is a post script to run
                if settings.post_script is not None:
                    p = subprocess.Popen([settings.post_script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    (out, err) = p.communicate()
                    if p.returncode != 0:
                        print("Post-script error")
                        print(out)
                        print(err)
                        sys.exit(9)
                    else:
                        print(out)
                if settings.sleep is False:
                    logger.info("Process completed!")
                    compress_log()
                    sys.exit(0)
                else:
                    logger.info("Sleeping for {} secs".format(settings.sleep))
                    # Sleep before trying again
                    time.sleep(settings.sleep)
                    continue
            except KeyboardInterrupt:
                logger.info("Ctrl-c detected. Leaving program.")
                compress_log()
                sys.exit(0)
            except Exception as e:
                logger.error("There was an error: {}".format(e))
                compress_log()
                sys.exit(1)


sys.exit(0)
