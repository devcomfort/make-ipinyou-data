"""Core utilities for manipulating the iPinYou dataset.

Currently exposes helpers for downloading and decompressing raw archives.
"""

from .unzip_bz2 import unzip_bz2

__all__ = ["unzip_bz2"]

