# Functions for osprey_worker.py
from datetime import datetime
import os
import subprocess
import xmltodict
import sys
import json
import requests
import pandas as pd
from random import randint
import glob
from PIL import Image
from pathlib import Path
import shutil
import locale
import itertools
import hashlib
from multiprocessing import Pool
# import pytesseract
import tarfile

# Zoom images
import si_deepzoom as deepzoom
# Get settings and queries
import settings

# Remove DecompressionBombWarning due to large files
# by using a large threshold
# https://github.com/zimeon/iiif/issues/11
Image.MAX_IMAGE_PIXELS = 1000000000


def check_requirements(program):
    """
    Check if required programs are installed
    """
    # From https://stackoverflow.com/a/34177358
    from shutil import which
    return which(program) is not None


def compress_log():
    """
    Compress log files
    """
    filecheck_dir = os.path.dirname(__file__)
    os.chdir(f"{filecheck_dir}/logs")
    folders = []
    files = []
    for entry in os.scandir('.'):
        if entry.is_dir():
            folders.append(entry.path)
        elif entry.is_file():
            files.append(entry.path)
    # No folders found
    if len(folders) == 0:
        return None
    # Compress each folder
    for folder in folders:
        subprocess.run(["zip", "-r", f"{folder}.zip", folder])
        shutil.rmtree(folder)
    for file in files:
        subprocess.run(["zip", f"{file}.zip", file])
        shutil.rmtree(file)
    os.chdir(filecheck_dir)
    return True


def send_request(url, payload, logger, log_res = True):
    """
    Execute request to API
    """
    try:
        logger.info(f"send_request: {url}|{payload}")        
        r = requests.post(url, data=payload)
        results = json.loads(r.text.encode('utf-8'))
        if r.status_code == 200:
            if log_res:
                logger.info(f"send_request_res: {results}")
            return results
        else:
            logger.error(f"send_request: {url}|{payload}|{r.headers}")
            return False
    except:
        logger.error(f"send_request: {url}|{payload}|{r.headers}")
        return False


def jhove_validate(file_path):
    """
    Validate the file with JHOVE
    """
    # Where to write the results
    xml_file = f"{settings.tmp_folder}/jhove_{randint(100, 100000)}.xml"
    if os.path.isfile(xml_file):
        os.unlink(xml_file)
    file_suffix = Path(file_path).suffix
    if file_suffix.lower() == ".tif":
        jhove_module = "TIFF-hul"
    elif file_suffix.lower() == ".jpg" or file_suffix.lower() == ".jpeg":
        jhove_module = "JPEG-hul"
    elif file_suffix.lower() == ".jp2":
        jhove_module = "JPEG2000-hul"
    elif file_suffix.lower() == ".png":
        jhove_module = "PNG-gdm"
    else:
        jhove_module = "BYTESTREAM"
        # return 1, "Unknown file type"
    p = subprocess.Popen([settings.jhove, "-h", "xml", "-o", xml_file, "-m", jhove_module, file_path],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    (out, err) = p.communicate()
    # Open and read the results xml
    try:
        with open(xml_file) as fd:
            doc = xmltodict.parse(fd.read())
    except Exception as e:
        # Try again
        if os.path.isfile(xml_file):
            os.unlink(xml_file)
        p = subprocess.Popen([settings.jhove, "-h", "xml", "-o", xml_file, "-m", jhove_module, file_path],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        (out, err) = p.communicate()
        # Open and read the results xml
        try:
            with open(xml_file) as fd:
                doc = xmltodict.parse(fd.read())
        except Exception as e:
            error_msg = f"Could not find result file from JHOVE ({xml_file}) ({e}) | {out} - {err}"
            check_results = 1
            check_info = error_msg
            return check_results, check_info
    if os.path.isfile(xml_file):
        os.unlink(xml_file)
    # Get file status
    file_status = doc['jhove']['repInfo']['status']
    jhove_results = out.decode('latin-1')
    if file_status == "Well-Formed and valid":
        check_results = 0
        check_info = jhove_results
    else:
        check_results = 1
        # If the only error is with the WhiteBalance, ignore
        # Issue open at Github, seems will be fixed in future release
        # https://github.com/openpreserve/jhove/issues/364
        if type(doc['jhove']['repInfo']['messages']['message']) is dict:
            # Single message
            if doc['jhove']['repInfo']['messages']['message']['#text'][:31] == "WhiteBalance value out of range":
                check_results = 0
                file_status = doc['jhove']['repInfo']['messages']['message']['#text']
            elif doc['jhove']['repInfo']['messages']['message']['#text'][:20] == "Unknown TIFF IFD tag":
                check_results = 0
                file_status = doc['jhove']['repInfo']['messages']['message']['#text']
            else:
                check_results = 1
                check_info = jhove_results
                file_status = doc['jhove']['repInfo']['messages']['message']['#text']
        else:
            if len(doc['jhove']['repInfo']['messages']['message']) == 2:
                if doc['jhove']['repInfo']['messages']['message'][0]['#text'][:20] == "Unknown TIFF IFD tag" and doc['jhove']['repInfo']['messages']['message'][1]['#text'][:31] == "WhiteBalance value out of range":
                    check_results = 0
                    f_stat = []
                    for msg in doc['jhove']['repInfo']['messages']['message']:
                        f_stat.append(msg['#text'])
                    file_status = ", ".join(f_stat)
            else:
                check_results = 1
                check_info = jhove_results
        check_info = f"{file_status}; {jhove_results}"
    return check_results, check_info


def magick_validate(filename, paranoid=False):
    """
    Validate the file with Imagemagick
    """
    try:
        settings.magick_limit
    except NameError:
        magick_limit = ""
    else:
        magick_limit = "MAGICK_THREAD_LIMIT={settings.magick_limit}"
    if paranoid:
        if settings.magick is None:
            p = subprocess.Popen([magick_limit, 'identify', '-verbose', '-regard-warnings', filename], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, shell=True)
        else:
            p = subprocess.Popen([magick_limit, settings.magick, '-verbose', '-regard-warnings', filename], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, shell=True)
    else:
        if settings.magick is None:
            p = subprocess.Popen([magick_limit, 'identify', '-verbose', filename], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, shell=True)
        else:
            p = subprocess.Popen([magick_limit, settings.magick, '-verbose', filename], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=True)
    (out, err) = p.communicate()
    if p.returncode == 0:
        check_results = 0
    else:
        check_results = 1
    magick_identify_info = out + err
    check_info = magick_identify_info.decode('latin-1')
    return check_results, check_info


def check_sequence(filename, project_files, sequence, sequence_split):
    filename_stem = Path(filename).stem
    file_id = None
    for file in project_files:
        if file['file_name'] == filename_stem:
            file_id = file['file_id']
            file_info = file
            break
    if file_id is None:
        # Something is wrong
        check_results = 1
        check_info = f"file_id not found for {filename}"
        return (file_id, check_results, check_info)
    file_suffix = filename_stem.split(sequence_split)
    file_wo_suffix = file_suffix[0:len(file_suffix) - 1]
    file_wo_suffix = '_'.join(file_wo_suffix)
    file_suffix = file_suffix[len(file_suffix) - 1]
    # Found last in sequence
    if file_suffix == sequence[len(sequence) - 1]:
        # End of sequence
        check_results = 0
        check_info = "File is the first one in the sequence"
        return (file_id, check_results, check_info)
    for i in range(len(sequence)):
        if file_suffix == sequence[i]:
            next_in_seq = sequence[i + 1]
            next_filename_stem = "{}{}{}".format(file_wo_suffix, sequence_split, next_in_seq)
            for file in project_files:
                if file['file_name'] == next_filename_stem:
                    check_results = 0
                    check_info = f"Next file in sequence ({next_filename_stem}) found"
                    return (file_id, check_results, check_info)
    check_results = 1
    check_info = "Next file in sequence was not found"
    return (file_id, check_results, check_info)


def sequence_validate(filename, folder_id, project_files):
    """
    Validate that a suffix sequence is not missing items
    """
    sequence = settings.sequence
    sequence_split = settings.sequence_split
    file_id, check_results, check_info = check_sequence(filename, project_files, sequence, sequence_split)
    file_check = 'sequence'
    payload = {'type': 'file',
               'property': 'filechecks',
               'folder_id': folder_id,
               'file_id': file_id,
               'api_key': settings.api_key,
               'file_check': file_check,
               'value': check_results,
               'check_info': check_info
               }
    r = requests.post(f"{settings.api_url}/api/update/{settings.project_alias}", data=payload)
    query_results = json.loads(r.text.encode('utf-8'))
    if query_results["result"] is not True:
        return False
    return True


def tif_compression(file_path):
    """
    Check if the image has LZW compression
    """
    try:
        img = Image.open(file_path)
    except Exception as e:
        return 1, f"File opening error: {file_path} - {e}"
    check_info = img.info['compression']
    if check_info == 'tiff_lzw':
        check_results = 0
    else:
        check_results = 1
    # return True
    return check_results, check_info


# def check_img_bits(file_path):
#     """
#     Check if the image is in the required 8- or 16-bits
#     """
#     try:
#         settings.magick_limit
#     except NameError:
#         magick_limit = ""
#     else:
#         magick_limit = "MAGICK_THREAD_LIMIT={}".format(settings.magick_limit)    
#     if settings.magick is None:
#         p = subprocess.Popen([magick_limit, 'identify', file_path], stdout=subprocess.PIPE,
#                             stderr=subprocess.PIPE, shell=True)
#     else:
#         p = subprocess.Popen([magick_limit, settings.magick, file_path], stdout=subprocess.PIPE,
#                         stderr=subprocess.PIPE, shell=True)
#     (out, err) = p.communicate()
#     if p.returncode == 0:
#         check_results = 0
#     else:
#         check_results = 1
#     magick_identify_info = out + err
#     check_info = magick_identify_info.decode('latin-1')
#     return check_results, check_info


# def tesseract(file_path):
#     """
#     Check if the text in the image has correct orientation and extract text with tesseract
#     """
#     img = Image.open(file_path)
#     try:
#         osd = pytesseract.image_to_osd(img, output_type='dict')
#     except pytesseract.pytesseract.TesseractError as e:
#         return 0, "Tesseract error: {}".format(e)
#     if osd['rotate'] != 0 and osd['orientation_conf'] > 0.5:
#         check_results = 1
#         check_info = "rotation:{}({})|{}".format(osd['rotate'], osd['orientation_conf'], pytesseract.pytesseract.image_to_string(img.rotate(osd['rotate'])).lstrip().rstrip().encode('utf-8'))
#     else:
#         check_results = 0
#         check_info = "rotation:{}({})|{}".format(osd['rotate'], osd['orientation_conf'], pytesseract.pytesseract.image_to_string(img).lstrip().rstrip().encode('utf-8'))
#     return check_results, check_info


def tifpages(file_path):
    """
    Check if TIF has multiple pages using Pillow
    """
    try:
        img = Image.open(file_path)
    except Exception as e:
        return 1, f"File opening error: {file_path} - {e}"
    no_pages = img.n_frames
    if no_pages == 1:
        check_results = 0
    else:
        check_results = 1
    # return True
    return check_results, f"No. of pages: {no_pages}"


def get_file_exif(filename):
    """
    Extract the EXIF info from the RAW file
    """
    p = subprocess.Popen([settings.exiftool, '-j', '-L', '-a', '-U', '-u', '-D', '-G1', filename],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = p.communicate()
    exif_info = out
    return exif_info


def get_filemd5(filepath, logger):
    """
    Get MD5 hash of a file
    """
    md5_hash = hashlib.md5()
    if os.path.isfile(filepath):
        with open(filepath, "rb") as f:
            # Read and update hash in chunks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
        file_md5 = md5_hash.hexdigest()
    else:
        return False
    return file_md5


def file_pair_check(filename, raw_files):
    """
    Check if a file has a pair (main + raw)
    """
    file_stem = Path(filename).stem
    # Check if file pair is present
    pair_files = []
    for rfile in raw_files:
        if file_stem == Path(rfile).stem:
            # Raw found
            pair_files.append(rfile)
    return pair_files


def jpgpreview(file_id, folder_id, file_path, logger):
    """
    Create preview image
    """
    if settings.jpg_previews == "":
        logger.error("JPG preview folder is not set in settings file")
        return False
    if settings.jpg_previews_free != None:
        disk_check = shutil.disk_usage(settings.jpg_previews)
        if (disk_check.free / disk_check.total) < settings.jpg_previews_free:
            logger.error(f"JPG storage location is running out of space ({round(disk_check.free / disk_check.total, 4) * 100}%) - {settings.jpg_previews}")
            return False
    preview_file_path = f"{settings.jpg_previews}/folder{folder_id}"
    preview_image_160 = f"{preview_file_path}/160/{file_id}.jpg"
    # Create subfolder if it doesn't exists
    os.makedirs(preview_file_path, exist_ok=True)
    resized_preview_file_path = f"{preview_file_path}/160"
    os.makedirs(resized_preview_file_path, exist_ok=True)
    try:
        img = Image.open(file_path)
    except Exception as e:
        logger.error(f"File opening error: {file_path} - {e}")
        return False
    original_profile = img.info.get("icc_profile")
    # 160
    width = 160
    width_o, height_o = img.size
    height = round(height_o * (width / width_o))
    newsize = (width, height)
    im1 = img.resize(newsize)
    im1.save(preview_image_160, 'jpeg', icc_profile=original_profile, quality=100)
    if os.path.isfile(preview_image_160) is False:
        logger.error(f"File: {file_path}")
        return False
    return True


def jpgpreview_zoom(file_id, folder_id, file_path, logger):
    """
    Create preview image with zoom-in capabilities
    """
    if settings.jpg_previews == "":
        logger.error("JPG preview folder is not set in settings file")
        return False
    if settings.jpg_previews_free != None:
        disk_check = shutil.disk_usage(settings.jpg_previews)
        if (disk_check.free / disk_check.total) < settings.jpg_previews_free:
            logger.error(f"JPG storage location is running out of space ({round(disk_check.free / disk_check.total, 4) * 100}%) - {settings.jpg_previews}")
            return False
    preview_file_path = f"{settings.jpg_previews}/folder{folder_id}"
    zoom_folder = f"{preview_file_path}/{file_id}_files"
    # Remove tiles folder
    if os.path.exists(zoom_folder):
        shutil.rmtree(zoom_folder, ignore_errors=True)
    # Create subfolder if it doesn't exists
    os.makedirs(preview_file_path, exist_ok=True)
    os.makedirs(zoom_folder, exist_ok=True)
    # deepzoom
    creator = deepzoom.ImageCreator(tile_size=254,
                           tile_format='jpg',
                           image_quality=1.0,
                           resize_filter='antialias')
    creator.create(file_path, f"{preview_file_path}/{file_id}.dzi")
    logger.info(f"Tar of previews of {file_id} ({preview_file_path})")
    try:
        os.chdir(preview_file_path)
        file_prev_folder = f"{file_id}_files"
        tar = tarfile.open(f"{preview_file_path}/{file_id}_files.tar", "w")
        tar.add(file_prev_folder)
        tar.close()
        shutil.rmtree(zoom_folder)
    except Exception as e:
        logger.error(f"Error tar for {file_id} ({e})")
        return False
    return True


def md5sum(md5_hashes, file):
    # https://stackoverflow.com/a/7829658
    filename = Path(file).name
    md5_hash = hashlib.md5()
    with open(file, "rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
    file_md5 = md5_hash.hexdigest()
    try:
        md5_from_file = md5_hashes.loc[md5_hashes['file'] == filename, 'md5'].item()
    except ValueError:
        try:
            md5_from_file = md5_hashes.loc[md5_hashes['filename'] == filename, 'md5'].item()
        except ValueError:
            # An error getting or matching the value
            return 1
    try:
        if file_md5.upper() == md5_from_file.upper():
            return 0
        elif md5_from_file == 'Series([], )':
            return 1
        else:
            return 1
    except AttributeError:
        return 1


def check_md5(md5_hashes, files):
    """
    Compare hashes between files and what the md5 file says
    :param md5_file:
    :param files:
    :return:
    """
    inputs = zip(itertools.repeat(md5_hashes), files)
    with Pool(settings.no_workers) as pool:
        bad_files = pool.starmap(md5sum, inputs)
        pool.close()
        pool.join()
    if sum(bad_files) > 0:
        return 1, f"{sum(bad_files)} Files Don't Match MD5 File"
    else:
        return 0, 0


def validate_md5(md5_files, files):
    """
    Check if the MD5 files are valid
    """
    md5_hashes = pd.DataFrame(columns=['md5', 'file'])
    for md5f in md5_files:
        # Read md5 file
        read_md5 = pd.read_csv(md5f, sep=' ', header=None, names=['md5', 'file'])
        md5_hashes = pd.concat([md5_hashes, read_md5], ignore_index=True, sort=False)
    if len(files) != md5_hashes.shape[0]:
        exit_msg = f"No. of files ({len(files)}) mismatch MD5 file ({md5_hashes.shape[0]})"
        return 1, exit_msg
    md5_hashes['filename'] = md5_hashes.apply(lambda row: Path(row.file).name, axis=1)
    res, results = check_md5(md5_hashes, files)
    if res == 0:
        exit_msg = "Valid MD5"
        return 0, exit_msg
    else:
        exit_msg = results
        return 1, exit_msg



def update_folder_stats(folder_id, logger):
    """
    Update the stats for the folder
    """
    payload = {'type': 'folder',
               'folder_id': folder_id,
               'api_key': settings.api_key,
               'property': 'stats',
               'value': '0'
               }
    r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
    if r is False:
        logger.error(r)
        return False
    else:
        return True


def run_checks_folder_p(project_info, folder_path, logfile_folder, logger):
    """
    Process a folder in parallel
    """
    project_id = project_info['project_alias']
    default_payload = {'api_key': settings.api_key}
    project_info = send_request(f"{settings.api_url}/api/projects/{settings.project_alias}", default_payload, logger, log_res = False)
    if project_info == False:
        return False
    project_checks = project_info['project_checks']
    logger.info(f"Processing folder: {folder_path}")
    folder_name = os.path.basename(folder_path)
    # Check if the folder exists in the database
    folder_id = None
    if len(project_info['folders']) > 0:
        for folder in project_info['folders']:
            logger.info(f"folder: {folder}")
            logger.info("FOLDER NEW: {}|{}|{}|{}|{}|{}".format(folder['folder'], folder_name, folder['folder_path'], folder_path, folder['folder'] == folder_name, folder['folder_path'] == folder_path))
            if folder['folder'] == folder_name: # and folder['folder_path'] == folder_path:
                folder_info = folder
                folder_id = folder_info['folder_id']
                delivered_to_dams = folder_info['delivered_to_dams']
                logger.info(f"Folder exists: {folder_id}")
                # folder found, break loop
                break
    if folder_id is None:
        # CREATE FOLDER
        folder_date = settings.folder_date(folder_name)
        payload = {
            'type': 'folder',
            'api_key': settings.api_key,
            'folder': folder_name,
            'folder_path': folder_path,
            'folder_date': folder_date,
            'project_id': project_info['project_id']
        }
        r = send_request(f"{settings.api_url}/api/new/{settings.project_alias}", payload, logger)
        if r is False:
            return False
        else:
            folder_id = r["result"][0]['folder_id']
            delivered_to_dams = 9
    # if folder_id is None:
    if 'folder_id' not in locals():
        logger.error(f"Could not get folder_id for {folder_name}")
        return False
    # Check if folder is ready or in DAMS
    if delivered_to_dams == 0 or delivered_to_dams == 1:
        # Folder ready for or delivered to DAMS, skip
        logger.info(f"Folder ready for or delivered to for DAMS, skipping {folder_path}")
        return folder_id
    # Check if QC has been run
    folder_info = send_request(f"{settings.api_url}/api/folders/{folder_id}", default_payload, logger, log_res = False)
    if folder_info is False:
        return False
    if folder_info['qc_status'] != "QC Pending":
        # QC done, so skip
        logger.info(f"Folder QC has been completed, skipping {folder_path}")
        return folder_id
    if folder_info['status'] == 0 and folder_info['file_errors'] == 0 and settings.run_once is True:
        # Folder done, so skip
        logger.info(f"Folder has been completed, skipping {folder_path}")
        return folder_id
    # Tag folder as under verification
    payload = {'type': 'folder',
               'folder_id': folder_id,
               'api_key': settings.api_key,
               'property': 'checking_folder',
               'value': 1
               }
    r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
    if r is False:
        return False
    # Get all files in folder
    files = []
    for root, d_names, f_names in os.walk(folder_path):
        for f in f_names:
            files.append(os.path.join(root, f))
    # Extraneous files?
    image_files = []
    image_main_files = []
    md5_allowed_files = []
    if 'raw_pair' in project_checks:
        allowed_files = [settings.md5_file, settings.main_files, settings.raw_files]
        allowed_image_files = [settings.main_files, settings.raw_files]
        md5_files = [settings.main_files, settings.raw_files]
    else:
        allowed_files = [settings.md5_file, settings.main_files]
        allowed_image_files = [settings.main_files]
        md5_files = [settings.main_files]
    if settings.data_files != None:
        allowed_files = [settings.md5_file, settings.main_files, settings.data_files]
        md5_files = [settings.main_files, settings.data_files]
    for file in files:
        if Path(file).suffix not in allowed_files:
            payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key, 'property': 'status1', 'value': 'Extraneous files'}
            r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
            if r is False:
                return False
            return False
        else:
            if Path(file).suffix in md5_files:
                md5_allowed_files.append(file)
            if Path(file).suffix in allowed_image_files:
                image_files.append(file)
            if Path(file).suffix == settings.main_files:
                image_main_files.append(file)
    # Check for deleted files
    for file in folder_info['files']:
        total = 0
        for f in image_main_files:
            f_name = Path(f).stem
            if f_name == file['file_name']:
                total += 1
        if total == 0:
            # File not found, delete from db
            payload = {'type': 'file',
                    'file_id': file['file_id'],
                    'api_key': settings.api_key,
                    'property': 'delete',
                    'value': True
                    }
            r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
            if r is False:
                return False
        elif total > 1:
            payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key, 'property': 'status1', 'value': 'Dupe file in folder ({})'.format(f_name)}
            r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
            if r is False:
                return False
    # Check if filenames have spaces
    for file in files:
        if " " in file:
            payload = {'type': 'folder',
               'folder_id': folder_id,
               'api_key': settings.api_key,
               'property': 'filename_spaces',
               'value': 1
               }
            r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
            if r is False:
                return False
            # Update folder stats
            update_folder_stats(folder_id, logger)
            # Don't do anything else
            return False
    # MD5 required?
    if settings.md5_required:
        md5_files = []
        for f in files:
            if Path(f).suffix == settings.md5_file:
                md5_files.append(f)
        # Check if MD5 exists in tif folder
        if len(md5_files) == 0:
            folder_status_msg = "MD5 files missing"
            payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key, 'property': 'status1', 'value': folder_status_msg}
            r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
            if r is False:
                return False
            # Update folder stats
            update_folder_stats(folder_id, logger)
            # Don't do anything else
            return False
        else:
            # Check if the MD5 file matches the contents of the folder
            md5_check, md5_error = validate_md5(md5_files, md5_allowed_files)
            if md5_check == 0:
                property = 'tif_md5_matches_ok'
            else:
                property = 'tif_md5_matches_error'
            payload = {'type': 'folder',
                    'folder_id': folder_id,
                    'api_key': settings.api_key,
                    'property': property,
                    'value': md5_error
                    }
            r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
            if r is False:
                return False
            payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key, 'property': 'status1', 'value': md5_error}
            if md5_check != 0:
                r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
                if r is False:
                    return False
    if 'raw_pair' in project_checks:
        raw_files = [file for file in files if Path(file).suffix == settings.raw_files]
        if len(image_main_files) != len(raw_files):
            folder_status_msg = f"No. of files do not match (main: {len(image_main_files)}, raws: {len(raw_files)})"
            payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key, 'property': 'status1',
                        'value': folder_status_msg}
            r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
            if r is False:
                return False
    else:
        raw_files = []
    # Create subfolder if it doesn't exists
    if len(image_main_files) > 0:
        preview_file_path = f"{settings.jpg_previews}/folder{folder_id}"
        if not os.path.exists(preview_file_path):
            os.makedirs(preview_file_path)
    ###############
    # Parallel
    ###############
    no_tasks = len(image_main_files)
    if settings.no_workers == 1:
        print_str = "Started run of {notasks} tasks for {folder_path}"
        print_str = print_str.format(notasks=str(locale.format_string("%d", no_tasks, grouping=True)), folder_path=folder_path)
        logger.info(print_str)
        # Process files in parallel
        for file in image_main_files:
            res = process_image_p(file, folder_id, raw_files, logfile_folder)
            if res is False:
                return False
    else:
        print_str = "Started parallel run of {notasks} tasks on {workers} workers for {folder_path}"
        print_str = print_str.format(notasks=str(locale.format_string("%d", no_tasks, grouping=True)), workers=str(
            settings.no_workers), folder_path=folder_path)
        logger.info(print_str)
        # Process files in parallel
        inputs = zip(image_main_files, itertools.repeat(folder_id), itertools.repeat(raw_files), itertools.repeat(logfile_folder))
        with Pool(settings.no_workers) as pool:
            pool.starmap(process_image_p, inputs)
            pool.close()
            pool.join()
    # Run end-of-folder checks
    if 'sequence' in project_checks:
        no_tasks = len(image_main_files)
        project_files = send_request(f"{settings.api_url}/api/projects/{settings.project_alias}/files", default_payload, logger, log_res = False)
        if project_files is False:
            return False
        if settings.no_workers == 1:
            print_str = "Started run of {notasks} tasks for 'sequence'"
            print_str = print_str.format(notasks=str(locale.format_string("%d", no_tasks, grouping=True)))
            logger.info(print_str)
            # Process files in parallel
            for file in image_main_files:
                sequence_validate(file, folder_id, project_files)
        else:
            print_str = "Started parallel run of {notasks} tasks on {workers} workers for 'sequence'"
            print_str = print_str.format(notasks=str(locale.format_string("%d", no_tasks, grouping=True)), workers=str(settings.no_workers))
            logger.info(print_str)
            # Process files in parallel
            inputs = zip(image_main_files, itertools.repeat(folder_id), itertools.repeat(project_files))
            with Pool(settings.no_workers) as pool:
                pool.starmap(sequence_validate, inputs)
                pool.close()
                pool.join()
    # Verify numbers match
    logger.info(f"Folder count verification {folder_id}")
    folder_info = send_request(f"{settings.api_url}/api/folders/{folder_id}", default_payload, logger, log_res = False)
    no_files_api = len(folder_info['files'])
    no_files_main = len(image_main_files)
    logger.info(f"Folder numbers match: (folder_id:{folder_id}) {no_files_main}/{no_files_api}")
    if no_files_api != no_files_main:
        logger.error(f"Files in system ({no_files_main}) do not match files in API ({no_files_api})")
        payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key, 'property': 'status1', 'value': "System error"}
        r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
        if r["result"] is not True:
            return False
    else:
        logger.info(f"Files in system ({no_files_main}) match files in API ({no_files_api}) (folder_id:{folder_id})")
        payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key, 'property': 'status0', 'value': ""}
        r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
        if r is False:
            return False
    # Update folder stats
    update_folder_stats(folder_id, logger)
    # If same server, set previews as ready
    if settings.previews is True:
        payload = {'type': 'folder', 'folder_id': folder_id, 'api_key': settings.api_key, 'property': 'previews', 'value': '0'}
        r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
        if r is False:
            return False
    logger.info(f"Folder {folder_path} completed")
    return folder_id


def process_image_p(filename, folder_id, raw_files, logfile_folder):
    """
    Run checks for image files
    """
    import settings
    import random
    import logging
    import time
    # import subprocess
    import requests
    random_int = random.randint(1, 1000)
    # Logging
    current_time = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    logfile = f"{logfile_folder}/{current_time}_{random_int}.log"
    logging.basicConfig(filename=logfile, filemode='a', level=logging.DEBUG,
                        format='%(levelname)s | %(asctime)s | %(filename)s:%(lineno)s | %(message)s',
                        datefmt='%y-%b-%d %H:%M:%S')
    logger = logging.getLogger(f"osprey_{random_int}")
    main_file_path = filename
    logger.info(f"filename: {main_file_path}")
    folder_id = int(folder_id)
    filename_stem = Path(filename).stem
    filename_suffix = Path(filename).suffix[1:]
    file_name = Path(filename).name
    # Copy to tmp folder
    # osprey_wd = "{}/ospreyp_{}".format(settings.tmp_folder, random.randint(100,10000))
    # try:
    #     os.mkdir(osprey_wd)
    # except FileExistsError as error:
    #     # Try another name
    #     osprey_wd = "{}/ospreyp_{}b".format(settings.tmp_folder, random.randint(100,10000))
    #     os.mkdir(osprey_wd)
    osprey_wd = settings.tmp_folder
    # main_file_path = f"{osprey_wd}/{file_name}"
    # shutil.copy(main_file_path, main_file_path)
    default_payload = {'api_key': settings.api_key}
    # Get folder info
    folder_info = send_request(f"{settings.api_url}/api/folders/{folder_id}", default_payload, logger, log_res = False)
    if folder_info is False:
        return False
    # Get project info
    project_info = send_request(f"{settings.api_url}/api/projects/{settings.project_alias}", default_payload, logger)
    if project_info is False:
        return False
    project_checks = project_info['project_checks']
    logger.info(f"project_checks: {project_checks}")
    # Check if file exists, insert if not
    file_id = None
    for file in folder_info['files']:
        if file['file_name'] == filename_stem:
            file_id = file['file_id']
            file_info = file
            break
    if file_id is None:
        # Get modified date for file
        file_timestamp_float = os.path.getmtime(main_file_path)
        file_timestamp = datetime.fromtimestamp(file_timestamp_float).strftime('%Y-%m-%d %H:%M:%S')
        payload = {
                'api_key': settings.api_key,
                'type': "file",
                'folder_id': folder_id,
                'filename': filename_stem,
                'timestamp': file_timestamp,
                'filetype': filename_suffix.lower(),
                }
        file_info = send_request(f"{settings.api_url}/api/new/{settings.project_alias}", payload, logger)
        if file_info is False:
            return False
        else:
            file_info = file_info['result']
        logging.debug("new_file:{}".format(file_info))
        file_id = file_info[0]['file_id']
        file_uid = file_info[0]['uid']
        logging.debug("file_size_pre: {}".format(main_file_path))
        file_size = os.path.getsize(main_file_path)
        logging.debug("file_size: {} {}".format(main_file_path, file_size))
        filetype = filename_suffix
        payload = {
            'api_key': settings.api_key,
            'type': "filesize",
            'file_id': file_id,
            'filetype': filetype.lower(),
            'filesize': file_size
        }
        r = send_request(f"{settings.api_url}/api/new/{settings.project_alias}", payload, logger)
        if r is False:
            return False
        # Refresh folder info
        folder_info = send_request(f"{settings.api_url}/api/folders/{folder_id}", default_payload, logger, log_res = False)
        if folder_info is False:
            return False
        for file in folder_info['files']:
            if file['file_name'] == filename_stem:
                file_id = file['file_id']
                file_info = file
                break
    # File exists, tag if there is a dupe
    payload = {'type': 'file',
                'property': 'unique',
                'folder_id': folder_id,
                'file_id': file_id,
                'api_key': settings.api_key,
                'file_check': 'unique_file',
                'value': True,
                'check_info': True
                }
    folder_info = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
    if folder_info is False:
        return False
    # Check if there is a dupe in another project
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
        r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
        if r is False:
            return False
    logging.info(f"file_info: {file_id} - {file_info}")
    # Generate jpg preview, if needed
    jpg_prev = jpgpreview(file_id, folder_id, main_file_path, logger)
    logger.info(f"jpg_prev: {file_id} {main_file_path} {jpg_prev}")
    if jpg_prev is False:
        return False
    # Generate zoomable jpg preview
    jpg_prev = jpgpreview_zoom(file_id, folder_id, main_file_path, logger)
    if jpg_prev is False:
        return False
    logger.info(f"jpgpreview_zoom: {file_id} {main_file_path} {jpg_prev}")
    file_md5 = get_filemd5(main_file_path, logger)
    if file_md5 is False:
        return False    
    logger.info(f"file_md5: {file_id} {main_file_path} - {file_md5}")
    payload = {'type': 'file',
               'property': 'filemd5',
               'file_id': file_id,
               'api_key': settings.api_key,
               'filetype': filename_suffix,
               'value': file_md5
               }
    r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
    if r is False:
        logger.error(r)
        return False
    # Get exif from TIF
    data = get_file_exif(main_file_path)
    payload = {'type': 'file',
               'property': 'exif',
               'file_id': file_id,
               'api_key': settings.api_key,
               'filetype': filename_suffix.lower(),
               'value': data
               }
    r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger, log_res = False)
    if r is False:
        logger.error(r)
        return False
    logger.info(f"Running checks on file {filename_stem} ({file_id}; folder_id: {folder_id})")
    # Run each check
    if 'raw_pair' in project_checks:
        file_check = 'raw_pair'
        # FilePair check and get MD5 hash
        paired_files = file_pair_check(file_name, raw_files)
        if len(paired_files) == 0:
            check_results = 1
            check_info = f"Raw file not found for {filename_stem} ({file_id})"
        elif len(paired_files) > 1:
            check_results = 1
            check_info = f"{len(paired_files)} raw files found for {filename_stem} ({file_id})"
        else:
            check_results = 0
            check_info = f"Raw file {Path(paired_files[0]).name} found for {filename} ({file_id})."
            # Copy raw to tmp
            raw_file = paired_files[0]
            # tmp_folder_rawfile = f"{osprey_wd}/{Path(raw_file).name}"
            # try:
            #     shutil.copy(raw_file, tmp_folder_rawfile)
            # except shutil.Error as e:
            #     logger.error(f"Error copying {raw_file} to {tmp_folder_rawfile}")
            #     # shutil.rmtree(osprey_wd, ignore_errors=True)
            #     return False
            rawfile_suffix = Path(raw_file).suffix[1:]
            check_results1, check_info1 = jhove_validate(raw_file)
            check_results2, check_info2 = magick_validate(raw_file)
            res = ""
            if check_results1 == 1:
                res1 = f"JHOVE could not validate: {check_info1}"
                check_results1 = 1
            else:
                res1 = f"JHOVE validated the file: {check_info1}"
                check_results1 = 0
            if check_results2 == 1:
                if rawfile_suffix == "eip":
                    check_results2 = 0
                    res2 = ""
                else:
                    res2 = f"Imagemagick could not validate: {check_info2}"
                    check_results2 = 1
            else:
                res2 = f"Imagemagick validated the file: {check_info2}"
                check_results2 = 0
            if (check_results1 + check_results2) > 0:
                check_results = 1
            payload = {'type': 'file',
                    'property': 'filechecks',
                    'folder_id': folder_id,
                    'file_id': file_id,
                    'api_key': settings.api_key,
                    'file_check': file_check,
                    'value': check_results,
                    'check_info': "{}; {}; {}".format(check_info, res1, res2).replace(settings.project_datastorage, "")
                    }
            r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
            if r is False:
                return False
            # MD5 of RAW file
            file_md5 = get_filemd5(raw_file, logger)
            if file_md5 is False:
                return False
            raw_filetype = Path(raw_file).suffix[1:]
            logging.debug("raw_file_md5: {} {} ({})".format(Path(raw_file).stem, file_md5, file_id))
            payload = {'type': 'file',
                    'property': 'filemd5',
                    'file_id': file_id,
                    'api_key': settings.api_key,
                    'filetype': raw_filetype.lower(),
                    'value': file_md5
                    }
            r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
            if r is False:
                return False
            # Raw file size
            file_size = os.path.getsize(raw_file)
            logging.debug(f"raw_file_size: {Path(raw_file).stem} {file_size} ({file_id})")
            raw_filetype = Path(raw_file).suffix[1:]
            payload = {
                'api_key': settings.api_key,
                'type': "filesize",
                'file_id': file_id,
                'filetype': raw_filetype.lower(),
                'filesize': file_size
            }
            r = send_request(f"{settings.api_url}/api/new/{settings.project_alias}", payload, logger)
            if r is False:
                return False
    if 'jhove' in project_checks:
        file_check = 'jhove'
        check_results, check_info = jhove_validate(main_file_path)
        payload = {'type': 'file',
                   'property': 'filechecks',
                   'folder_id': folder_id,
                   'file_id': file_id,
                   'api_key': settings.api_key,
                   'file_check': file_check,
                   'value': check_results,
                   'check_info': check_info.replace(settings.project_datastorage, "")
                   }
        r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
        if r is False:
            return False
    # if 'bits' in project_checks:
    #     file_check = 'bits'
    #     check_results, check_info = check_img_bits(main_file_path)
    #     payload = {'type': 'file',
    #                'property': 'filechecks',
    #                'folder_id': folder_id,
    #                'file_id': file_id,
    #                'api_key': settings.api_key,
    #                'file_check': file_check,
    #                'value': check_results,
    #                'check_info': check_info.replace(settings.project_datastorage, "")
    #                }
    #     r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
    #     if r is False:
    #         shutil.rmtree(tmp_folder)
    #         return False
    if 'filename' in project_checks:
        file_check = 'filename'
        payload = {'type': 'file',
                   'property': 'filechecks',
                   'folder_id': folder_id,
                   'file_id': file_id,
                   'api_key': settings.api_key,
                   'file_check': file_check,
                   'value': check_results,
                   'check_info': check_info.replace(settings.project_datastorage, "")
                   }
        r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
        if r is False:
            return False
    if 'tifpages' in project_checks:
        file_check = 'tifpages'
        logger.info("tifpages_pre: {} {}".format(file_id, main_file_path))
        check_results, check_info = tifpages(main_file_path)
        logger.info("tifpages: {} {} {}".format(file_id, check_results, check_info))
        payload = {'type': 'file',
                   'property': 'filechecks',
                   'folder_id': folder_id,
                   'file_id': file_id,
                   'api_key': settings.api_key,
                   'file_check': file_check,
                   'value': check_results,
                   'check_info': check_info
                   }
        r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
        if r is False:
            return False
    if 'magick' in project_checks:
        file_check = 'magick'
        check_results, check_info = magick_validate(main_file_path)
        if check_results != 0:
            logger.error("magick error: {}".format(check_info))
            return False
        payload = {'type': 'file',
                   'property': 'filechecks',
                   'folder_id': folder_id,
                   'file_id': file_id,
                   'api_key': settings.api_key,
                   'file_check': file_check,
                   'value': check_results,
                   'check_info': check_info.replace(settings.project_datastorage, "")
                   }
        r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
        if r is False:
            return False
    if 'tif_compression' in project_checks:
        file_check = 'tif_compression'
        check_results, check_info = tif_compression(main_file_path)
        logger.info(f"tif_compression: {file_id} {check_results} {check_info}")
        payload = {'type': 'file',
                   'property': 'filechecks',
                   'folder_id': folder_id,
                   'file_id': file_id,
                   'api_key': settings.api_key,
                   'file_check': file_check,
                   'value': check_results,
                   'check_info': check_info
                   }
        r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
        if r is False:
            return False
    # if 'tesseract' in project_checks:
    #     file_check = 'tesseract'
    #     heck_results = 0
    #     check_info = "Not checked"
    #     for patterns in settings.tesseract_pattern:
    #         if patterns in Path(main_file_path).stem:
    #             check_results, check_info = tesseract(main_file_path)
    #             break
    #     logger.info(f"tesseract: {file_id} {check_results} {check_info}")
    #     payload = {'type': 'file',
    #                'property': 'filechecks',
    #                'folder_id': folder_id,
    #                'file_id': file_id,
    #                'api_key': settings.api_key,
    #                'file_check': file_check,
    #                'value': check_results,
    #                'check_info': check_info
    #                }
    #     r = send_request(f"{settings.api_url}/api/update/{settings.project_alias}", payload, logger)
    #     if r is False:
    #         return False
    return folder_id

