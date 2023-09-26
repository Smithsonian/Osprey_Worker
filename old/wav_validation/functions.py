# Functions for MDfilecheck.py
import datetime
import os
import subprocess
import re
import xmltodict
import sys
import settings
from random import randint
import queries
# For MD5
import hashlib
import glob
from PIL import Image
from subprocess import PIPE
from pathlib import Path
import shutil


def check_requirements(program):
    """
    Check if required programs are installed
    """
    # From https://stackoverflow.com/a/34177358
    from shutil import which
    return which(program) is not None


def compress_log(filecheck_dir):
    """
    Compress log files
    """
    os.chdir('{}/logs'.format(filecheck_dir))
    for file in glob.glob('*.log'):
        subprocess.run(["zip", "{}.zip".format(file), file])
        os.remove(file)
    os.chdir(filecheck_dir)
    return True


def check_folder(folder_name, folder_path, project_id, db_cursor):
    """
    Check if a folder exists, add if it does not
    """
    if settings.folder_name == "server_folder":
        server_folder_path = folder_path.split("/")
        len_server_folder_path = len(server_folder_path)
        folder_name = "{}/{}".format(server_folder_path[len_server_folder_path - 2],
                                     server_folder_path[len_server_folder_path - 1])
    db_cursor.execute(queries.select_folderid,
                      {'project_folder': folder_name, 'folder_path': folder_path, 'project_id': project_id})
    folder_id = db_cursor.fetchone()
    if folder_id is None:
        # Folder does not exists, create
        db_cursor.execute(queries.new_folder,
                          {'project_folder': folder_name, 'folder_path': folder_path, 'project_id': project_id})
        folder_id = db_cursor.fetchone()
    folder_date = settings.folder_date(folder_name)
    db_cursor.execute(queries.folder_date, {'datequery': folder_date, 'folder_id': folder_id[0]})
    return folder_id[0]


def delete_folder_files(folder_id, db_cursor, logger):
    db_cursor.execute(queries.del_folder_files, {'folder_id': folder_id})
    logger.debug(db_cursor.query.decode("utf-8"))
    return True


def folder_updated_at(folder_id, db_cursor, logger):
    """
    Update the last time the folder was checked
    """
    db_cursor.execute(queries.folder_updated_at, {'folder_id': folder_id})
    logger.debug(db_cursor.query.decode("utf-8"))
    return True


def file_updated_at(file_id, db_cursor, logger):
    """
    Update the last time the file was checked
    """
    db_cursor.execute(queries.file_updated_at, {'file_id': file_id})
    logger.debug(db_cursor.query.decode("utf-8"))
    return True


def jhove_validate(file_id, filename, db_cursor, logger):
    """
    Validate the file with JHOVE
    """
    # Where to write the results
    xml_file = "{}/mdpp_{}.xml".format(settings.tmp_folder, randint(100, 100000))
    if os.path.isfile(xml_file):
        os.unlink(xml_file)
    # Run JHOVE
    subprocess.run([settings.jhove_path, "-h", "xml", "-o", xml_file, filename])
    # Open and read the results xml
    try:
        with open(xml_file) as fd:
            doc = xmltodict.parse(fd.read())
    except Exception as e:
        error_msg = "Could not find result file from JHOVE ({}) ({})".format(xml_file, e)
        db_cursor.execute(queries.file_check,
                          {'file_id': file_id, 'file_check': 'jhove', 'check_results': 9, 'check_info': error_msg})
        return False
    if os.path.isfile(xml_file):
        os.unlink(xml_file)
    # Get file status
    file_status = doc['jhove']['repInfo']['status']
    if file_status == "Well-Formed and valid":
        jhove_val = 0
    else:
        jhove_val = 1
        if len(doc['jhove']['repInfo']['messages']) == 1:
            # If the only error is with the WhiteBalance, ignore
            # Issue open at Github, seems will be fixed in future release
            # https://github.com/openpreserve/jhove/issues/364
            if doc['jhove']['repInfo']['messages']['message']['#text'][:31] == "WhiteBalance value out of range":
                jhove_val = 0
        file_status = doc['jhove']['repInfo']['messages']['message']['#text']
    db_cursor.execute(queries.file_check, {'file_id': file_id, 'file_check': 'jhove', 'check_results': jhove_val,
                                           'check_info': file_status})
    logger.debug(db_cursor.query.decode("utf-8"))
    return True


def magick_validate(file_id, filename, db_cursor, logger, paranoid=False):
    """
    Validate the file with Imagemagick
    """
    if paranoid:
        p = subprocess.Popen(['identify', '-verbose', '-regard-warnings', filename], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
    else:
        p = subprocess.Popen(['identify', '-verbose', filename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = p.communicate()
    if p.returncode == 0:
        magick_identify = 0
    else:
        magick_identify = 1
    magick_identify_info = out + err
    db_cursor.execute(queries.file_check, {'file_id': file_id, 'file_check': 'magick', 'check_results': magick_identify,
                                           'check_info': magick_identify_info.decode('latin-1')})
    logger.debug(db_cursor.query.decode("utf-8"))
    return True


def valid_name(file_id, filename, db_cursor, logger):
    """
    Check if filename in database of accepted names
    """
    db_cursor.execute(settings.filename_pattern_query.format(Path(filename).stem))
    valid_names = db_cursor.fetchone()[0]
    if valid_names == 0:
        filename_check = 1
        filename_check_info = "Filename {} not in list".format(Path(filename).stem)
    else:
        filename_check = 0
        filename_check_info = "Filename {} in list".format(Path(filename).stem)
    db_cursor.execute(queries.file_check,
                      {'file_id': file_id, 'file_check': 'valid_name', 'check_results': filename_check,
                       'check_info': filename_check_info})
    logger.debug(db_cursor.query.decode("utf-8"))
    return True


def file_exif(file_id, filename, filetype, db_cursor, logger):
    """
    Extract the EXIF info from the RAW file
    """
    p = subprocess.Popen(['exiftool', '-t', '-a', '-U', '-u', '-D', '-G1', '-s', filename], stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    (out, err) = p.communicate()
    if p.returncode == 0:
        exif_read = 0
    else:
        exif_read = 1
    exif_info = out
    for line in exif_info.splitlines():
        # Non utf, ignore for now
        try:
            tag = re.split(r'\t+', line.decode('UTF-8'))
            db_cursor.execute(queries.save_exif,
                              {'file_id': file_id, 'filetype': filetype, 'taggroup': tag[0], 'tagid': tag[1],
                               'tag': tag[2], 'value': tag[3]})
            logger.debug(db_cursor.query.decode("utf-8"))
        except:
            logger.error("Tag not in utf-8 for file {}, {} {} {}".format(file_id, tag[0], tag[1], tag[2]))
            continue
    return True


def itpc_validate(file_id, filename, db_cursor):
    """
    Check the IPTC Metadata
    2Do
    """
    return False


def file_size_check(filename, filetype, file_id, db_cursor, logger):
    """
    Check if a file is within the size limits
    """
    import bitmath
    file_size = os.path.getsize(filename)
    if filetype == "tif":
        if file_size < settings.tif_size_min:
            file_size = 1
            file_size_info = "TIF file is smaller than expected ({})".format(
                bitmath.getsize(filename, system=bitmath.SI))
        elif file_size > settings.tif_size_max:
            file_size = 1
            file_size_info = "TIF file is larger than expected ({})".format(
                bitmath.getsize(filename, system=bitmath.SI))
        else:
            file_size = 0
            file_size_info = "{}".format(bitmath.getsize(filename, system=bitmath.SI))
        file_check = 'tif_size'
    elif filetype == "raw":
        if file_size < settings.raw_size_min:
            file_size = 1
            file_size_info = "RAW file is smaller than expected ({})".format(
                bitmath.getsize(filename, system=bitmath.SI))
        elif file_size > settings.raw_size_max:
            file_size = 1
            file_size_info = "RAW file is larger than expected ({})".format(
                bitmath.getsize(filename, system=bitmath.SI))
        else:
            file_size = 0
            file_size_info = "{}".format(bitmath.getsize(filename, system=bitmath.SI))
        file_check = 'raw_size'
    db_cursor.execute(queries.file_check, {'file_id': file_id, 'file_check': file_check, 'check_results': file_size,
                                           'check_info': file_size_info})
    logger.debug(db_cursor.query.decode("utf-8"))
    return True


def filemd5(filepath):
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
        file_md5 = ""
    return file_md5


def file_pair_check(file_id, filename, tif_path, file_tif, raw_path, file_raw, db_cursor, logger):
    """
    Check if a file has a pair (tif + raw)
    """
    file_stem = Path(filename).stem
    # Check if file pair is present
    tif_file = "{}/{}.{}".format(tif_path, file_stem, file_tif)
    raw_file = "{}/{}.{}".format(raw_path, file_stem, file_raw)
    if os.path.isfile(tif_file) is False:
        # Tif file is missing
        file_pair = 1
        file_pair_info = "Missing tif"
    elif os.path.isfile(raw_file) is False:
        # Raw file is missing
        file_pair = 1
        file_pair_info = "Missing {} file".format(settings.raw_files)
    else:
        file_pair = 0
        file_pair_info = "tif and {} found".format(settings.raw_files)
    db_cursor.execute(queries.file_check, {'file_id': file_id, 'file_check': 'raw_pair', 'check_results': file_pair,
                                           'check_info': file_pair_info})
    logger.debug(db_cursor.query.decode("utf-8"))
    return True


def soxi_check(file_id, filename, file_check, expected_val, db_cursor, logger):
    """
    Get the tech info of a wav file
    """
    if file_check == "filetype":
        fcheck = "t"
    elif file_check == "samprate":
        fcheck = "r"
    elif file_check == "channels":
        fcheck = "c"
    elif file_check == "duration":
        fcheck = "D"
    elif file_check == "bits":
        fcheck = "b"
    else:
        # Unknown check
        return False
    p = subprocess.Popen(['soxi', '-{}'.format(fcheck), filename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = p.communicate()
    result = out.decode("utf-8").replace('\n', '')
    err = err.decode("utf-8").replace('\n', '')
    if file_check == "filetype":
        if result == expected_val:
            result_code = 0
        else:
            result_code = 1
    elif file_check == "samprate":
        if result == expected_val:
            result_code = 0
        else:
            result_code = 1
    elif file_check == "channels":
        if result == expected_val:
            result_code = 0
        else:
            result_code = 1
    elif file_check == "duration":
        if result == expected_val:
            result_code = 0
        else:
            result_code = 1
    elif file_check == "bits":
        if result == expected_val:
            result_code = 0
        else:
            result_code = 1
    db_cursor.execute(queries.file_check, {'file_id': file_id, 'file_check': file_check, 'check_results': result_code,
                                           'check_info': result})
    logger.debug(db_cursor.query.decode("utf-8"))
    return True


def checkmd5file(md5_file, folder_id, filetype, db_cursor, logger):
    """
    Check if md5 hashes match with the files
    -In progress
    """
    md5_error = ""
    if filetype == "tif":
        db_cursor.execute(queries.select_tif_md5, {'folder_id': folder_id, 'filetype': 'tif'})
    elif filetype == "raw":
        db_cursor.execute(queries.select_tif_md5, {'folder_id': folder_id, 'filetype': 'raw'})
    logger.debug(db_cursor.query.decode("utf-8"))
    import pandas
    vendor = pandas.DataFrame(db_cursor.fetchall(), columns=['md5_1', 'filename'])
    md5file = pandas.read_csv(md5_file, header=None, names=['md5_2', 'filename'], index_col=False, sep="  ")
    # Remove suffix
    if filetype == "tif":
        md5file['filename'] = md5file['filename'].str.replace(".tif", "")
        md5file['filename'] = md5file['filename'].str.replace(".TIF", "")
    elif filetype == "raw":
        md5file['filename'] = md5file['filename'].str.replace(".{}".format(settings.raw_files.lower()), "")
        md5file['filename'] = md5file['filename'].str.replace(".{}".format(settings.raw_files.upper()), "")
    md5check = pandas.merge(vendor, md5file, how="outer", on="filename")
    # MD5 hashes don't match
    # Get rows where MD5 don't match
    md5check_match = md5check[md5check.md5_1 != md5check.md5_2]
    # Ignore NAs
    md5check_match = md5check_match.dropna()
    # check if there are any mismatches
    nrows = len(md5check_match)
    if nrows > 0:
        md5_error = md5_error + "There were {} files where the MD5 hash did not match:".format(nrows)
        for i in range(0, nrows):
            md5_error = md5_error + "\n - File: {}, MD5 of file: {}, hash in file: {}".format(
                md5check_match['filename'][i], md5check_match['md5_2'], md5check_match['md5_1'])
    # Extra files in vendor mount
    vendor_extras = vendor[~vendor.filename.isin(md5file.filename)]['filename']
    # Extra files in md5file
    md5file_extras = md5file[~md5file.filename.isin(vendor.filename)]['filename']
    return True


def check_deleted(filetype, db_cursor, logger):
    """
    Deleted files are tagged in the database
    """
    # Get path
    if filetype == 'tif':
        files_path = settings.tif_files_path
    elif filetype == 'wav':
        files_path = settings.wav_files_path
    elif filetype == 'raw':
        files_path = settings.raw_files_path
    elif filetype == 'jpg':
        files_path = settings.jpg_files_path
    else:
        return False
    db_cursor.execute(queries.get_files, {'project_id': settings.project_id})
    logger.debug(db_cursor.query.decode("utf-8"))
    files = db_cursor.fetchall()
    for file in files:
        if os.path.isdir("{}/{}/".format(file[2], files_path)):
            if os.path.isfile("{}/{}/{}.{}".format(file[2], files_path, file[1], filetype)):
                file_exists = 0
                file_exists_info = "File {}/{}/{}.{} was found".format(file[2], files_path, file[1], filetype)
            else:
                file_exists = 1
                file_exists_info = "File {}/{}/{}.{} was not found, deleting".format(file[2], files_path, file[1],
                                                                                     filetype)
                db_cursor.execute(queries.delete_file, {'file_id': file[0]})
                logger.debug(db_cursor.query.decode("utf-8"))
            logger.info(file_exists_info)
    return True



def update_folder_stats(folder_id, db_cursor, logger):
    """
    Update the stats for the folder
    """
    db_cursor.execute(queries.update_nofiles, {'folder_id': folder_id})
    logger.debug(db_cursor.query.decode("utf-8"))
    db_cursor.execute(queries.get_fileserrors, {'folder_id': folder_id})
    logger.debug(db_cursor.query.decode("utf-8"))
    no_errors = db_cursor.fetchone()[0]
    db_cursor.execute(queries.get_filespending, {'folder_id': folder_id})
    logger.debug(db_cursor.query.decode("utf-8"))
    no_pending = db_cursor.fetchone()[0]
    if no_errors > 0:
        f_errors = 1
    else:
        if no_pending > 0:
            f_errors = 9
        else:
            f_errors = 0
    db_cursor.execute(queries.update_folder_errors, {'folder_id': folder_id, 'f_errors': f_errors})
    logger.debug(db_cursor.query.decode("utf-8"))
    return True


def process_wav(filename, folder_path, folder_id, db_cursor, logger):
    """
    Run checks for wav files
    """
    folder_id = int(folder_id)
    tmp_folder = "{}/mdpp_wav_{}".format(settings.tmp_folder, str(folder_id))
    if os.path.isdir(tmp_folder):
        shutil.rmtree(tmp_folder, ignore_errors=True)
    os.mkdir(tmp_folder)
    filename_stem = Path(filename).stem
    # Check if file exists, insert if not
    logger.info("WAV file {}".format(filename))
    q_checkfile = queries.select_file_id.format(filename_stem, folder_id)
    logger.info(q_checkfile)
    db_cursor.execute(q_checkfile)
    file_id = db_cursor.fetchone()
    if file_id is None:
        file_timestamp_float = os.path.getmtime("{}/{}".format(folder_path, filename))
        file_timestamp = datetime.fromtimestamp(file_timestamp_float).strftime('%Y-%m-%d %H:%M:%S')
        db_cursor.execute(queries.insert_file,
                          {'file_name': filename_stem, 'folder_id': folder_id, 'unique_file': unique_file,
                           'file_timestamp': file_timestamp})
        logger.debug(db_cursor.query.decode("utf-8"))
        file_id = db_cursor.fetchone()[0]
    else:
        file_id = file_id[0]
    logger.info("filename: {} with file_id {}".format(filename_stem, file_id))
    # Check if file is OK
    file_checks = 0
    for filecheck in settings.project_file_checks:
        db_cursor.execute(queries.select_check_file, {'file_id': file_id, 'filecheck': filecheck})
        logger.debug(db_cursor.query.decode("utf-8"))
        result = db_cursor.fetchone()
        if result[0] is not None:
            file_checks = file_checks + result[0]
    if file_checks == 0:
        file_updated_at(file_id, db_cursor, logger)
        logger.info("File with ID {} is OK, skipping".format(file_id))
        return True
    else:
        # Checks that do not need a local copy
        if 'valid_name' in settings.project_file_checks:
            db_cursor.execute(queries.select_check_file, {'file_id': file_id, 'filecheck': 'old_name'})
            logger.debug(db_cursor.query.decode("utf-8"))
            result = db_cursor.fetchone()[0]
            if result != 0:
                valid_name(file_id, local_tempfile, db_cursor, logger)
        if 'unique_file' in settings.project_file_checks:
            db_cursor.execute(queries.select_check_file, {'file_id': file_id, 'filecheck': 'old_name'})
            logger.debug(db_cursor.query.decode("utf-8"))
            result = db_cursor.fetchone()[0]
            if result != 0:
                db_cursor.execute(queries.check_unique, {'file_name': filename_stem, 'folder_id': folder_id,
                                                         'project_id': settings.project_id})
                logger.debug(db_cursor.query.decode("utf-8"))
                result = db_cursor.fetchone()
                if result[0] > 0:
                    unique_file = 1
                else:
                    unique_file = 0
                db_cursor.execute(queries.file_check,
                                  {'file_id': file_id, 'file_check': 'unique_file', 'check_results': unique_file,
                                   'check_info': ''})
                logger.debug(db_cursor.query.decode("utf-8"))
        if 'old_name' in settings.project_file_checks:
            db_cursor.execute(queries.select_check_file, {'file_id': file_id, 'filecheck': 'old_name'})
            logger.debug(db_cursor.query.decode("utf-8"))
            result = db_cursor.fetchone()[0]
            if result != 0:
                db_cursor.execute(queries.check_unique_old, {'file_name': filename_stem, 'folder_id': folder_id,
                                                             'project_id': settings.project_id})
                logger.debug(db_cursor.query.decode("utf-8"))
                result = db_cursor.fetchall()
                if len(result) > 0:
                    old_name = 1
                    folders = ",".join(result[0])
                else:
                    old_name = 0
                    folders = ""
                db_cursor.execute(queries.file_check,
                                  {'file_id': file_id, 'file_check': 'old_name', 'check_results': old_name,
                                   'check_info': folders})
                logger.debug(db_cursor.query.decode("utf-8"))
        # Checks that DO need a local copy
        # Check if there is enough space first
        local_disk = shutil.disk_usage(settings.tmp_folder)
        if (local_disk.free / local_disk.total < 0.1):
            logger.error(
                "Disk is running out of space {} ({})".format(local_disk.free / local_disk.total, settings.tmp_folder))
            sys.exit(1)
        logger.info("Copying file {} to local tmp".format(filename))
        # Copy file to tmp folder
        local_tempfile = "{}/{}".format(tmp_folder, filename)
        try:
            shutil.copyfile("{}/{}/{}".format(folder_path, settings.wav_files_path, filename), local_tempfile)
        except:
            logger.error("Could not copy file {}/{}/{} to local tmp".format(folder_path, settings.wav_files_path, filename))
            db_cursor.execute(queries.file_exists, {'file_exists': 1, 'file_id': file_id})
            logger.debug(db_cursor.query.decode("utf-8"))
            # return False
            sys.exit(1)
        # Compare MD5 between source and copy
        sourcefile_md5 = filemd5("{}/{}".format(folder_path, filename))
        # Store MD5
        file_md5 = filemd5(local_tempfile)
        if sourcefile_md5 != file_md5:
            logger.error(
                "MD5 hash of local copy does not match the source: {} vs {}".format(sourcefile_md5, file_md5))
            return False
        db_cursor.execute(queries.save_md5, {'file_id': file_id, 'filetype': 'wav', 'md5': file_md5})
        logger.debug(db_cursor.query.decode("utf-8"))
        logger.info("wav_md5:{}".format(file_md5))
        if 'filetype' in settings.project_file_checks:
            db_cursor.execute(queries.select_check_file, {'file_id': file_id, 'filecheck': 'old_name'})
            logger.debug(db_cursor.query.decode("utf-8"))
            result = db_cursor.fetchone()[0]
            if result != 0:
                soxi_check(file_id, filename, "filetype", settings.wav_filetype, db_cursor, logger)
        if 'samprate' in settings.project_file_checks:
            db_cursor.execute(queries.select_check_file, {'file_id': file_id, 'filecheck': 'old_name'})
            logger.debug(db_cursor.query.decode("utf-8"))
            result = db_cursor.fetchone()[0]
            if result != 0:
                soxi_check(file_id, filename, "samprate", settings.wav_samprate, db_cursor, logger)
        if 'channels' in settings.project_file_checks:
            db_cursor.execute(queries.select_check_file, {'file_id': file_id, 'filecheck': 'old_name'})
            logger.debug(db_cursor.query.decode("utf-8"))
            result = db_cursor.fetchone()[0]
            if result != 0:
                soxi_check(file_id, filename, "channels", settings.wav_channels, db_cursor, logger)
        if 'bits' in settings.project_file_checks:
            db_cursor.execute(queries.select_check_file, {'file_id': file_id, 'filecheck': 'old_name'})
            logger.debug(db_cursor.query.decode("utf-8"))
            result = db_cursor.fetchone()[0]
            if result != 0:
                soxi_check(file_id, filename, "bits", settings.wav_bits, db_cursor, logger)
        if 'jhove' in settings.project_file_checks:
            db_cursor.execute(queries.select_check_file, {'file_id': file_id, 'filecheck': 'old_name'})
            logger.debug(db_cursor.query.decode("utf-8"))
            result = db_cursor.fetchone()[0]
            if result != 0:
                jhove_validate(file_id, local_tempfile, tmp_folder, db_cursor, logger)
        file_updated_at(file_id, db_cursor, logger)
        os.remove(local_tempfile)
        return True
