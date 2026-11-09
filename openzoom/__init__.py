#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
#  Deep Zoom Tools
#
#  Copyright (c) 2008-2011, OpenZoom <http://openzoom.org>
#  Copyright (c) 2008-2011, Daniel Gasienica <daniel@gasienica.ch>
#  Copyright (c) 2010, Boris Bluntschli <boris@bluntschli.ch>
#  Copyright (c) 2008, Kapil Thangavelu <kapil.foss@gmail.com>
#  All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without modification,
#  are permitted provided that the following conditions are met:
#
#      1. Redistributions of source code must retain the above copyright notice,
#         this list of conditions and the following disclaimer.
#
#      2. Redistributions in binary form must reproduce the above copyright
#         notice, this list of conditions and the following disclaimer in the
#         documentation and/or other materials provided with the distribution.
#
#      3. Neither the name of OpenZoom nor the names of its contributors may be used
#         to endorse or promote products derived from this software without
#         specific prior written permission.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
#  ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
#  ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

import math
import os
import PIL.Image
import xml.dom.minidom


NS_DEEPZOOM = 'http://schemas.microsoft.com/deepzoom/2008'

DEFAULT_IMAGE_FORMAT = 'jpg'

RESIZE_FILTERS = {
    'cubic': PIL.Image.BICUBIC,
    'bilinear': PIL.Image.BILINEAR,
    'bicubic': PIL.Image.BICUBIC,
    'nearest': PIL.Image.NEAREST,
    'antialias': PIL.Image.LANCZOS,
    }

IMAGE_FORMATS = {
    'jpg': 'jpg',
    'png': 'png',
    }


class DeepZoomImageDescriptor(object):
    def __init__(self, width=None, height=None,
                 tile_size=254, tile_overlap=1, tile_format='jpg'):
        self.width = width
        self.height = height
        self.tile_size = tile_size
        self.tile_overlap = tile_overlap
        self.tile_format = tile_format
        self._num_levels = None

    def save(self, destination):
        """Save descriptor file."""
        with open(destination, 'w') as f:
            doc = xml.dom.minidom.Document()
            image = doc.createElementNS(NS_DEEPZOOM, 'Image')
            image.setAttribute('xmlns', NS_DEEPZOOM)
            image.setAttribute('TileSize', str(self.tile_size))
            image.setAttribute('Overlap', str(self.tile_overlap))
            image.setAttribute('Format', str(self.tile_format))
            size = doc.createElementNS(NS_DEEPZOOM, 'Size')
            size.setAttribute('Width', str(self.width))
            size.setAttribute('Height', str(self.height))
            image.appendChild(size)
            doc.appendChild(image)
            descriptor = doc.toxml()
            f.write(descriptor)

    @property
    def num_levels(self):
        """Number of levels in the pyramid."""
        if self._num_levels is None:
            max_dimension = max(self.width, self.height)
            self._num_levels = int(math.ceil(math.log(max_dimension, 2))) + 1
        return self._num_levels

    def get_scale(self, level):
        """Scale of a pyramid level."""
        assert 0 <= level and level < self.num_levels, 'Invalid pyramid level'
        max_level = self.num_levels - 1
        return math.pow(0.5, max_level - level)

    def get_dimensions(self, level):
        """Dimensions of level (width, height)"""
        assert 0 <= level and level < self.num_levels, 'Invalid pyramid level'
        scale = self.get_scale(level)
        width = int(math.ceil(self.width * scale))
        height = int(math.ceil(self.height * scale))
        return (width, height)

    def get_num_tiles(self, level):
        """Number of tiles (columns, rows)"""
        assert 0 <= level and level < self.num_levels, 'Invalid pyramid level'
        w, h = self.get_dimensions( level )
        return (int(math.ceil(float(w) / self.tile_size)),
                int(math.ceil(float(h) / self.tile_size)))

    def get_tile_bounds(self, level, column, row):
        """Bounding box of the tile (x1, y1, x2, y2)"""
        assert 0 <= level and level < self.num_levels, 'Invalid pyramid level'
        offset_x = 0 if column == 0 else self.tile_overlap
        offset_y = 0 if row == 0 else self.tile_overlap
        x = (column * self.tile_size) - offset_x
        y = (row * self.tile_size) - offset_y
        level_width, level_height = self.get_dimensions(level)
        w = self.tile_size + (1 if column == 0 else 2) * self.tile_overlap
        h = self.tile_size + (1 if row == 0 else 2) * self.tile_overlap
        w = min(w, level_width  - x)
        h = min(h, level_height - y)
        return (x, y, x + w, y + h)


class ImageCreator(object):
    """Creates Deep Zoom images."""
    def __init__(self, tile_size=254, tile_overlap=1, tile_format='jpg',
                 image_quality=0.8, resize_filter=None, copy_metadata=False):
        self.tile_size = int(tile_size)
        self.tile_format = tile_format
        self.tile_overlap = _clamp(int(tile_overlap), 0, 10)
        self.image_quality = _clamp(image_quality, 0, 1.0)
        if not tile_format in IMAGE_FORMATS:
            self.tile_format = DEFAULT_IMAGE_FORMAT
        self.resize_filter = resize_filter
        self.copy_metadata = copy_metadata

    def get_image(self, level):
        """Returns the bitmap image at the given level."""
        assert 0 <= level and level < self.descriptor.num_levels, 'Invalid pyramid level'
        width, height = self.descriptor.get_dimensions(level)
        # don't transform to what we already have
        if self.descriptor.width == width and self.descriptor.height == height:
            return self.image
        if (self.resize_filter is None) or (self.resize_filter not in RESIZE_FILTERS):
            return self.image.resize((width, height), PIL.Image.LANCZOS)
        return self.image.resize((width, height), RESIZE_FILTERS[self.resize_filter])

    def tiles(self, level):
        """Iterator for all tiles in the given level. Returns (column, row) of a tile."""
        columns, rows = self.descriptor.get_num_tiles(level)
        return [(column, row) for column in range(columns) for row in range(rows)]

    def create(self, source, destination, image=None):
        """Creates Deep Zoom image from source file and saves it to destination.

        If image is given, it is used instead of opening source and the caller
        keeps ownership of it (it is not closed here). Pass an image in a mode
        the tile format can encode -- Pillow cannot write modes such as I;16
        (16-bit grayscale) as JPEG.
        """
        owns_image = image is None
        self.image = PIL.Image.open(source) if owns_image else image
        try:
            width, height = self.image.size
            self.descriptor = DeepZoomImageDescriptor(width=width,
                                                      height=height,
                                                      tile_size=self.tile_size,
                                                      tile_overlap=self.tile_overlap,
                                                      tile_format=self.tile_format)
            # Create tiles
            image_files = _get_or_create_path(_get_files_path(destination))
            icc_profile = self.image.info.get('icc_profile')
            for level in range(self.descriptor.num_levels):
                level_dir = _get_or_create_path(os.path.join(image_files, str(level)))
                level_image = self.get_image(level)
                for (column, row) in self.tiles(level):
                    bounds = self.descriptor.get_tile_bounds(level, column, row)
                    tile = level_image.crop(bounds)
                    format = self.descriptor.tile_format
                    tile_path = os.path.join(level_dir,
                                             '{}_{}.{}'.format(column, row, format))
                    with open(tile_path, 'wb') as tile_file:
                        if self.descriptor.tile_format == 'jpg':
                            jpeg_quality = int(self.image_quality * 100)
                            tile.save(tile_file, 'JPEG', quality=jpeg_quality, icc_profile=icc_profile)
                        else:
                            tile.save(tile_file, self.descriptor.tile_format.upper(), icc_profile=icc_profile)
            # Create descriptor
            self.descriptor.save(destination)
        finally:
            if owns_image:
                self.image.close()


################################################################################

def _get_or_create_path(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path

def _clamp(val, min, max):
    if val < min:
        return min
    elif val > max:
        return max
    return val

def _get_files_path(path):
    return os.path.splitext(path)[0] + '_files'
