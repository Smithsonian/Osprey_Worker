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

## License

Available under the Apache License 2.0. Consult the [LICENSE](LICENSE) file for details.
