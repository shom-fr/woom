#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Light weight workflow manager for ocean models
"""
from importlib.metadata import version as _version


try:
    __version__ = _version("xarray")
except Exception:
    # Local copy or not installed with setuptools.
    # Disable minimum version checks on downstream libraries.
    __version__ = "9999"


class WoomError(Exception):
    pass
