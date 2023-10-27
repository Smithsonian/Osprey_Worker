# Osprey Worker

Osprey is a system that checks the images produced by vendors in mass
digitization projects by the Collections Digitization program of the
Digitization Program Office, OCIO, Smithsonian.

![DPO Logo](https://github.com/Smithsonian/Osprey_Worker/assets/2302171/c45ba5d7-450f-4a06-84fd-6c2551dd21b8)

https://dpo.si.edu/

This repo contains the command line program `osprey_worker` that runs the checks on
the files from digitization projects. The program communicates with 
the Dashboard via an API.

The code for the Osprey Dashboard is in the repo
https://github.com/Smithsonian/Osprey

## Requirements

The program requires a modern version of Linux and Python 3.7 or newer. It has been used with Ubuntu 23 and 
RHEL 8. A server running [Osprey](https://github.com/Smithsonian/Osprey) is also required to write the data to.

To install the Python requirements, use pip:

```python
pip install -r requirements.txt
```

This includes the modules:

 * xmltodict
 * pandas
 * Pillow
 * requests

In addition, it requires these programs to be already installed in the system:

 * [JHOVE](https://jhove.openpreservation.org/)
 * [Imagemagick](https://imagemagick.org/)
 * [exiftool](https://exiftool.org/)

## Running

Rename the file `settings.py.template` to `settings.py` and update the values there. 

## License

Available under the Apache License 2.0. Consult the [LICENSE](LICENSE) file for details.
