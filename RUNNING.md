# Running Osprey Worker in a container

Build the image from the repository root (tag with the worker version to match the dashboard API):

```bash
docker build -t osprey-worker:3.0.0 .
# or
podman build -t osprey-worker:3.0.0 .
```

## Configuration

Copy `settings.py.template` to a host path (e.g. `/etc/osprey/projects/my_project/settings.py`) and set paths that exist inside the container mounts.

Pass the settings file via `--settings` or `OSPREY_SETTINGS`:

```bash
podman run --rm \
  --env OSPREY_SETTINGS=/config/settings.py \
  --volume /etc/osprey/projects/my_project/settings.py:/config/settings.py:ro \
  --volume /data/project_data:/data/project_data \
  --volume /data/osprey_previews:/data/osprey_previews \
  --volume /var/log/osprey/my_project/worker:/opt/osprey-worker/logs \
  osprey-worker:3.0.0 --worker-set 0
```

For rootless Podman, add `--userns=keep-id` so log files are owned by your user.

## CLI

```bash
python osprey_worker.py --help
python osprey_worker.py --version
python osprey_worker.py --settings /path/to/settings.py
python osprey_worker.py --settings /path/to/settings.py --worker-set 0
```

## Included tools

The image installs JHOVE (Java), exiftool, ImageMagick `identify`, and `zip` on PATH. Override tool paths in `settings.py` if needed.

## Production deployment

See the [osprey_service](https://github.com/Smithsonian/osprey_service) repository for `run_osprey_worker.sh`, `osprey-pipeline.sh`, and systemd timer units.
