#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Light weight workflow manager for ocean models
"""
import warnings

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "0.0.0"


class WoomError(Exception):
    pass


class WoomWarning(UserWarning):
    pass


class WoomDeprecationWarning(WoomWarning, DeprecationWarning):
    pass


def woom_warn(message, stacklevel=2, category=None):
    """Issue a :class:`WoomWarning` warning

    Example
    -------
    .. ipython:: python
        :okwarning:

        @suppress
        from woom import woom_warn
        woom_warn('Be careful!')
    """
    if category is None:
        category = WoomWarning
    elif category == "deprecation":
        category = WoomDeprecationWarning
    warnings.warn(message, category, stacklevel=stacklevel)
