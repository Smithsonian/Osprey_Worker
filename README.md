# Osprey Worker

Osprey is a system that checks the images produced by vendors in mass
digitization projects by the Collections Digitization program of the
Digitization Program Office, OCIO, Smithsonian.

![DPO Logo](https://github.com/Smithsonian/Osprey_Worker/assets/2302171/c45ba5d7-450f-4a06-84fd-6c2551dd21b8)

https://dpo.si.edu/

This repo contains the command line program `osprey_worker` that runs the checks on
the files from digitization projects. The program communicates with 
the Dashboard via an API.

Related repos:

 * https://github.com/Smithsonian/Osprey - Osprey Dashboard 
 * https://github.com/Smithsonian/Osprey_Worker/

## Layout

| Path | Purpose |
|------|---------|
| `osprey_worker.py` | Entry point |
| `regenerate_previews.py` | Regenerate previews for one already-registered folder (see [below](#regenerating-previews-for-one-folder)) |
| `worker/` | Validation pipeline, API client, previews, validators |
| `logutil.py` | Main and worker-process logging |
| `openzoom/` | Deep Zoom tile generation for zoomable previews |
| `settings.py.template` | Configuration template (copy to `settings.py`) |

## Requirements

The program requires a modern version of Linux and Python 3.7 or newer. It has been used with Ubuntu 23 and 
RHEL 8. A server running [Osprey](https://github.com/Smithsonian/Osprey) is also required to write the data to.

To install the Python requirements, use pip:

```python
pip install -r requirements.txt
```

Python packages:

 * xmltodict
 * Pillow
 * requests

External programs (must be on PATH or configured in `settings.py`):

 * [JHOVE](https://jhove.openpreservation.org/)
 * [ImageMagick](https://imagemagick.org/) (`identify`)
 * [exiftool](https://exiftool.org/)
 * `zip` (used to compress logs after each run)

## Configuration

Rename `settings.py.template` to `settings.py` and set at minimum:

 * `project_alias`, `api_url`, `api_key` — dashboard connection
 * `project_datastorage` — ingest folder containing one subdirectory per deliverable
 * `jpg_previews` — output path for JPG previews (thumbnails, optional full-size, optional Deep Zoom)
 * `main_files`, `raw_files`, `md5_file` — expected file extensions
 * `num_workers` — parallel processes for per-file work
 * `api_workers` — concurrent API POSTs when flushing worker results (default 8)
 * `zoom_workers` — max concurrent Deep Zoom jobs across workers (default 4; only when `deepzoom_previews` is True)
 * `deepzoom_previews` — generate Deep Zoom `.dzi` tiles for dashboard zoom UI (default False); also sets folder `preview_type` to `dzi` (otherwise `iiif`) when folder processing completes
 * `tar_previews` — archive Deep Zoom tiles into a single tar and remove the tiles folder; rarely needed (default False)
 * `regenerate_previews` — force preview rebuild when outputs exist (default False)
 * `preview_staging` — write previews via temp dir then move (default False, in-place)
 * `fullsize_jpg` — generate full-resolution JPEG per file (default True)
 * `profile_timing` — log per-phase elapsed times for diagnostics (default False)
 * `sequence`, `sequence_split` — required when the project enables sequence checks

See `settings.py.template` for all options.

## Running

```bash
python osprey_worker.py [--settings PATH] [--worker-set N]
```

 * `--settings` — path to `settings.py` (default: `$OSPREY_SETTINGS` or `./settings.py` in the current directory)
 * `--worker-set` — optional worker index. Worker `0` (or omitted) clears pending dashboard state at startup; other workers wait 20 seconds before processing.

For container deployment, see [RUNNING.md](RUNNING.md).

Logs are written under `logs/`:

 * Main run: `logs/{project_alias}_w{N}_{timestamp}.log` (or without `_w{N}` when no worker set)
 * Parallel file workers: `logs/workers.log`

Logs are zipped automatically when the script exits.

## Regenerating previews for one folder

`regenerate_previews.py` (re)creates the JPG thumbnails, optional full-size JPG, and
optional Deep Zoom previews for a single folder, without re-running validation checks
(JHOVE, MD5, sequence, etc.) on it. Use it to fix previews for a folder that has
already been processed by `osprey_worker.py` — e.g. after a bad Deep Zoom run, or
after changing `deepzoom_previews`/`fullsize_jpg` in `settings.py`.

```bash
python regenerate_previews.py [--settings PATH] <folder>
```

 * `<folder>` — the folder to regenerate, given either as the dashboard folder name
   (e.g. `20240115_vendorname`) or as a path (relative to `project_datastorage`, or
   absolute)
 * `--settings` — path to `settings.py` (default: `$OSPREY_SETTINGS` or `./settings.py`)

Requirements:

 * The folder must already be registered in the dashboard (i.e. `osprey_worker.py`
   has processed it at least once) — the script looks up its `folder_id` and each
   file's `file_id` via the dashboard API and does not create new records.
 * Same Python/package requirements as `osprey_worker.py` (JHOVE/exiftool/ImageMagick
   are not needed by this script; only Pillow and `requests`).

Behavior:

 * Only files matching `settings.main_files` in the folder (searched recursively) are
   processed.
 * Whether existing preview artifacts are rebuilt or left alone follows
   `settings.regenerate_previews`, exactly like `osprey_worker.py` — set it to `True`
   in `settings.py` to force a full rebuild, or leave it `False` to only fill in
   missing/incomplete previews.
 * Runs in parallel across `settings.num_workers` processes (same as the main
   pipeline) when greater than 1, otherwise sequentially.
 * Writes to the same `logs/` folder as `osprey_worker.py` (`logs/{project_alias}_regen_{timestamp}.log`
   and `logs/workers.log`), zipped on exit.
 * Exits non-zero if any file's previews failed to generate, or if the folder or any
   file isn't found in the dashboard.

## License

Available under the Apache License 2.0. Consult the [LICENSE](LICENSE) file for details.
