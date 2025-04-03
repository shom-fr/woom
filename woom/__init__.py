#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Light weight workflow manager for ocean models
"""

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "0.0.0"


class WoomError(Exception):
    pass
