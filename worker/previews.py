import os
import tarfile
import shutil
import tempfile
from pathlib import Path
from PIL import Image

import settings
import openzoom as deepzoom

_zoom_semaphore = None


def set_zoom_semaphore(semaphore):
    """Set a multiprocessing.Semaphore limiting concurrent Deep Zoom jobs (pool workers)."""
    global _zoom_semaphore
    _zoom_semaphore = semaphore


def resolve_preview_base(folder_id, jpg_previews, transcription=0):
    """
    Preview directory for a folder, aligned with pipeline layout.
    transcription=1 -> {jpg_previews}/{folder_id}
    otherwise        -> {jpg_previews}/folder{folder_id}
    """
    if transcription == 1:
        return f"{jpg_previews}/{folder_id}"
    return f"{jpg_previews}/folder{folder_id}"


def resolve_preview_type(deepzoom_previews=False):
    """Return dashboard preview_type: 'dzi' when Deep Zoom is enabled, else 'iiif'."""
    return 'dzi' if deepzoom_previews else 'iiif'


def _resolve_preview_path(folder_id, jpg_previews, transcription=0):
    """Returns (preview_base_path, is_transcription)."""
    preview_base = resolve_preview_base(folder_id, jpg_previews, transcription)
    return preview_base, transcription == 1


def _save_jpg_preview(img, preview_image, max_size, quality=92):
    original_profile = img.info.get("icc_profile")
    width_o, height_o = img.size
    if width_o == 0 or height_o == 0:
        raise ValueError("Image has zero width or height")
    ratio = min(max_size / width_o, max_size / height_o)
    newsize = (round(width_o * ratio), round(height_o * ratio))
    im1 = img.resize(newsize)
    if im1.mode not in ('RGB', 'L'):
        im1 = im1.convert('RGB')
    im1.save(preview_image, 'jpeg', icc_profile=original_profile, quality=quality)


def _save_fullsize_jpg(img, preview_path, quality=92):
    """Save full-resolution JPEG without resizing."""
    original_profile = img.info.get("icc_profile")
    if img.size[0] == 0 or img.size[1] == 0:
        raise ValueError("Image has zero width or height")
    out = img
    if out.mode not in ('RGB', 'L'):
        out = out.convert('RGB')
    out.save(preview_path, 'jpeg', icc_profile=original_profile, quality=quality)


def _fullsize_jpg_name(file_path):
    return f"{Path(file_path).stem}.jpg"


ALL_PREVIEW_ARTIFACTS = frozenset({'thumb_160', 'thumb_800', 'fullsize', 'zoom'})


def _expected_preview_artifacts(fullsize_jpg=True):
    """Return preview artifact names expected for current settings."""
    excluded = set()
    if not fullsize_jpg:
        excluded.add('fullsize')
    if not getattr(settings, 'deepzoom_previews', False):
        excluded.add('zoom')
    return ALL_PREVIEW_ARTIFACTS - frozenset(excluded)


def _zoom_complete(preview_base, file_id, tar_previews):
    """Return True when Deep Zoom descriptor and tile storage both exist."""
    if not os.path.isfile(os.path.join(preview_base, f'{file_id}.dzi')):
        return False
    if tar_previews:
        return os.path.isfile(os.path.join(preview_base, f'{file_id}_files.tar'))
    return os.path.isdir(os.path.join(preview_base, f'{file_id}_files'))


def missing_preview_artifacts(preview_base, file_id, file_path, tar_previews, fullsize_jpg=True):
    """Return names of expected preview artifacts that are not yet on disk."""
    expected = _expected_preview_artifacts(fullsize_jpg)
    missing = set()
    if 'thumb_160' in expected and not os.path.isfile(
        os.path.join(preview_base, '160', f'{file_id}.jpg')
    ):
        missing.add('thumb_160')
    if 'thumb_800' in expected and not os.path.isfile(
        os.path.join(preview_base, '800', f'{file_id}.jpg')
    ):
        missing.add('thumb_800')
    if 'fullsize' in expected and not os.path.isfile(
        os.path.join(preview_base, _fullsize_jpg_name(file_path))
    ):
        missing.add('fullsize')
    if 'zoom' in expected and not _zoom_complete(preview_base, file_id, tar_previews):
        missing.add('zoom')
    return missing


def previews_complete(preview_base, file_id, file_path, tar_previews, fullsize_jpg=True):
    """Return True if all expected preview artifacts already exist."""
    return not missing_preview_artifacts(
        preview_base, file_id, file_path, tar_previews, fullsize_jpg
    )


def _move_preview_tree(src_base, dest_base, file_id, tar_previews, fullsize_name=None,
                       artifacts=None):
    """Move generated preview artifacts from temp dir into final preview location."""
    os.makedirs(dest_base, exist_ok=True)
    move_all = artifacts is None
    if move_all or 'fullsize' in artifacts:
        if fullsize_name:
            src_file = os.path.join(src_base, fullsize_name)
            if os.path.isfile(src_file):
                shutil.move(src_file, os.path.join(dest_base, fullsize_name))
    for size, key in ((160, 'thumb_160'), (800, 'thumb_800')):
        if move_all or key in artifacts:
            src_dir = os.path.join(src_base, str(size))
            if os.path.isdir(src_dir):
                dest_dir = os.path.join(dest_base, str(size))
                os.makedirs(dest_dir, exist_ok=True)
                src_file = os.path.join(src_dir, f"{file_id}.jpg")
                if os.path.isfile(src_file):
                    shutil.move(src_file, os.path.join(dest_dir, f"{file_id}.jpg"))

    if move_all or 'zoom' in artifacts:
        dzi_src = os.path.join(src_base, f"{file_id}.dzi")
        if os.path.isfile(dzi_src):
            shutil.move(dzi_src, os.path.join(dest_base, f"{file_id}.dzi"))

        zoom_src = os.path.join(src_base, f"{file_id}_files")
        if os.path.isdir(zoom_src):
            zoom_dest = os.path.join(dest_base, f"{file_id}_files")
            if os.path.exists(zoom_dest):
                shutil.rmtree(zoom_dest, ignore_errors=True)
            shutil.move(zoom_src, zoom_dest)

        tar_src = os.path.join(src_base, f"{file_id}_files.tar")
        if tar_previews and os.path.isfile(tar_src):
            shutil.move(tar_src, os.path.join(dest_base, f"{file_id}_files.tar"))


def jpgpreview(file_id, folder_id, file_path, logger, jpg_previews, max_size=160,
               img=None, transcription=0):
    """
    Create preview image
    """
    if jpg_previews == "":
        logger.error("JPG preview folder is not set in settings file")
        return False
    try:
        preview_base, _ = _resolve_preview_path(folder_id, jpg_previews, transcription)
    except ValueError as e:
        logger.error(str(e))
        return False
    preview_image = f"{preview_base}/{max_size}/{file_id}.jpg"
    resized_preview_file_path = f"{preview_base}/{max_size}"
    os.makedirs(resized_preview_file_path, exist_ok=True)
    opened = img is None
    try:
        if opened:
            img = Image.open(file_path)
        _save_jpg_preview(img, preview_image, max_size)
    except Exception as e:
        logger.error(f"File opening error: {file_path} - {e}")
        return False
    finally:
        if opened and img is not None:
            img.close()
    if os.path.isfile(preview_image) is False:
        logger.error(f"File: {file_path}")
        return False
    return True


def jpgpreview_to_dir(file_id, img, dest_base, max_size):
    """Write a resized JPG preview into dest_base/{max_size}/{file_id}.jpg."""
    dest_dir = os.path.join(dest_base, str(max_size))
    os.makedirs(dest_dir, exist_ok=True)
    preview_image = os.path.join(dest_dir, f"{file_id}.jpg")
    _save_jpg_preview(img, preview_image, max_size)
    return preview_image


def jpgpreview_zoom_to_dir(file_id, file_path, logger, dest_base, tar_previews=False):
    """Create Deep Zoom preview under dest_base."""
    zoom_folder = os.path.join(dest_base, f"{file_id}_files")
    if os.path.exists(zoom_folder):
        shutil.rmtree(zoom_folder, ignore_errors=True)
    os.makedirs(dest_base, exist_ok=True)
    os.makedirs(zoom_folder, exist_ok=True)
    creator = deepzoom.ImageCreator(tile_size=254,
                           tile_format='jpg',
                           image_quality=1.0,
                           resize_filter='antialias')
    if _zoom_semaphore is not None:
        _zoom_semaphore.acquire()
    try:
        try:
            creator.create(file_path, os.path.join(dest_base, f"{file_id}.dzi"))
        except Exception as e:
            logger.error(f"deepzoom create failed for {file_id}: {e}")
            if os.path.exists(zoom_folder):
                shutil.rmtree(zoom_folder, ignore_errors=True)
            return False
    finally:
        if _zoom_semaphore is not None:
            _zoom_semaphore.release()
    if tar_previews:
        logger.info(f"Tar of previews of {file_id} ({dest_base})")
        try:
            file_prev_folder = f"{file_id}_files"
            with tarfile.open(os.path.join(dest_base, f"{file_id}_files.tar"), "w") as tar:
                tar.add(os.path.join(dest_base, file_prev_folder), arcname=file_prev_folder)
            shutil.rmtree(zoom_folder)
        except Exception as e:
            logger.error(f"Error tar for {file_id} ({e})")
            return False
    logger.info(f"Zoom preview created for {file_id}")
    return True


def _write_previews_to_dir(file_id, file_path, dest_base, logger, tar_previews,
                           fullsize_jpg, img=None, artifacts=None):
    """
    Write preview artifacts under dest_base.
    artifacts: subset of expected preview artifacts to generate; None means all expected.
    If img is provided the caller retains ownership (not closed here).
    """
    if artifacts is None:
        artifacts = _expected_preview_artifacts(fullsize_jpg)

    need_img = bool(artifacts & {'thumb_160', 'thumb_800', 'fullsize'})
    opened_here = img is None and need_img
    try:
        if need_img:
            if opened_here:
                try:
                    img = Image.open(file_path)
                except Exception as e:
                    logger.error(f"File opening error: {file_path} - {e}")
                    return False

            if 'thumb_160' in artifacts:
                jpgpreview_to_dir(file_id, img, dest_base, 160)
            if 'thumb_800' in artifacts:
                jpgpreview_to_dir(file_id, img, dest_base, 800)
            if 'fullsize' in artifacts and fullsize_jpg:
                fullsize_name = _fullsize_jpg_name(file_path)
                _save_fullsize_jpg(img, os.path.join(dest_base, fullsize_name))

        if 'zoom' in artifacts:
            if not jpgpreview_zoom_to_dir(
                file_id, file_path, logger, dest_base, tar_previews=tar_previews
            ):
                return False
        return True
    finally:
        if opened_here and img is not None:
            img.close()


def generate_previews(file_id, folder_id, file_path, logger, jpg_previews,
                      tar_previews=False, transcription=0, img=None):
    """
    Generate thumbnail, optional full-size JPG, and optional Deep Zoom previews.
    Deep Zoom is generated only when settings.deepzoom_previews is True.
    Skips artifacts that already exist unless settings.regenerate_previews is True.
    Writes in-place by default; set settings.preview_staging=True for temp-dir staging.
    """
    if jpg_previews == "":
        logger.error("JPG preview folder is not set in settings file")
        return False
    try:
        preview_base, _ = _resolve_preview_path(folder_id, jpg_previews, transcription)
    except ValueError as e:
        logger.error(str(e))
        return False

    fullsize_jpg = getattr(settings, 'fullsize_jpg', True)
    regenerate = getattr(settings, 'regenerate_previews', False)
    use_staging = getattr(settings, 'preview_staging', False)

    if regenerate:
        artifacts = _expected_preview_artifacts(fullsize_jpg)
    else:
        artifacts = missing_preview_artifacts(
            preview_base, file_id, file_path, tar_previews, fullsize_jpg
        )
        if not artifacts:
            logger.info(f"Skipping previews (already exist) for {file_id}")
            return True
        logger.info(f"Generating missing previews {sorted(artifacts)} for {file_id}")

    if use_staging:
        temp_base = tempfile.mkdtemp(prefix=f"osprey-{file_id}-")
        try:
            fullsize_name = (
                _fullsize_jpg_name(file_path)
                if fullsize_jpg and 'fullsize' in artifacts
                else None
            )
            if not _write_previews_to_dir(
                file_id, file_path, temp_base, logger, tar_previews, fullsize_jpg,
                img=img, artifacts=artifacts
            ):
                return False
            _move_preview_tree(
                temp_base, preview_base, file_id, tar_previews,
                fullsize_name=fullsize_name, artifacts=artifacts
            )
            return True
        finally:
            shutil.rmtree(temp_base, ignore_errors=True)

    os.makedirs(preview_base, exist_ok=True)
    return _write_previews_to_dir(
        file_id, file_path, preview_base, logger, tar_previews, fullsize_jpg,
        img=img, artifacts=artifacts
    )


def jpgpreview_zoom(file_id, folder_id, file_path, logger, jpg_previews,
                    tar_previews=False, transcription=0):
    """
    Create preview image with zoom-in capabilities.
    If tar_previews is True, tiles are tarred and the folder removed.
    """
    return generate_previews(
        file_id, folder_id, file_path, logger, jpg_previews, tar_previews, transcription
    )
