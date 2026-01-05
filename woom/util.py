#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misc utilities
"""
import collections
import json
import logging
import os
import re
import subprocess
import sys

import pandas as pd


class WoomDate(pd.Timestamp):
    """Extended pandas Timestamp with custom formatting and time arithmetic

    Parameters
    ----------
    date : str or datetime-like
        Date specification
    round : str, optional
        Frequency string for rounding
    """

    re_match_since = re.compile(r"^(years|months|days|hours|minutes|seconds)\s+since\s+(\d+.*)$", re.I).match
    # re_match_add = re.compile(r"^([+\-].+)$").match

    def __new__(cls, date, round=None):
        if isinstance(date, str) and date in ["now", "today"]:
            date = pd.to_datetime(date, utc=True)
        else:
            date = pd.to_datetime(date)
            if date.tzinfo is None:
                date = date.tz_localize("utc")
        # date = pd.to_datetime(date, utc=utc)
        # if utc:
        #     date = date.tz_localize(None)
        if round:
            date = date.round(round)
        instance = super().__new__(cls, date)
        instance.__class__ = cls
        return instance

    def __format__(self, spec):
        m = self.re_match_since(spec)
        if m:
            units, origin = m.groups()
            origin = pd.to_datetime(origin)
            if origin.tzinfo is None:
                origin = origin.tz_localize("utc")
            return "{:g}".format((self - pd.to_datetime(origin)) / pd.to_timedelta(1, units))

        return super().__format__(spec)

    def add(self, *args, **kwargs):
        """Add time delta"""
        date = self
        for arg in args:
            date = date + pd.to_timedelta(arg)
        for unit, value in kwargs.items():
            date = date + pd.to_timedelta(value, unit)
        return date


def check_dir(filepath, dry=False, logger=None):
    """Make sure that the directory that contains file exists

    Parameters
    ----------
    filepath : str
        File path
    dry : bool
        Fake mode. Do not create the directory
    logger : logging.Logger, optional
        To inform that we create the directory, even in dry mode

    Returns
    -------
    str
        Absolute file path
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    filepath = os.path.abspath(filepath)
    dirname = os.path.dirname(filepath)
    if not os.path.exists(dirname):
        if logger:
            logger.debug(f"Creating directory: {dirname}")
        if not dry:
            os.makedirs(dirname)
        if logger:
            logger.info(f"Created directory: {dirname}")
    return filepath


class WoomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for woom objects"""

    def default(self, obj):
        # Dict
        if isinstance(obj, collections.UserDict):
            return dict(obj)

        # Process
        if hasattr(obj, "pid") or isinstance(obj, subprocess.Popen):
            return obj.pid

        # Workflow and managers
        if hasattr(obj, "to_json_entry"):
            return obj.to_json_entry()

        try:
            return super().default(obj)
        except TypeError:
            return str(obj)


def params2env_vars(params=None, select=None, **extra_params):
    """Convert a dict of parameters to env vars whose name starts with WOOM_PARAMS_

    Parameters
    ----------
    params : dict, optional
        Parameters dictionary
    select : list, optional
        Keys to select from params
    **extra_params
        Additional parameters

    Returns
    -------
    dict
        Environment variables dictionary
    """
    if params is None:
        params = extra_params
    else:
        params = params.copy()
        params.update(extra_params)
    env_vars = {}
    for key, value in params.items():
        if select and key not in select:
            continue
        if isinstance(value, (pd.Timestamp, pd.Timedelta)):
            value = value.isoformat()
        if value is None:
            value = ""
        if isinstance(value, bool):
            value = str(int(value))
        env_vars["WOOM_PARAMS_" + key.upper()] = str(value)
    return env_vars


def pages2ints(pages, n):
    """Convert a list of 1-based integers and zero-based slices to a pure list of one-based integers

    Parameters
    ----------
    pages : list
        List of integers or slices
    n : int
        Total number of pages

    Returns
    -------
    list
        List of 1-based integer indices
    """
    out = []
    indices = [i + 1 for i in range(n)]
    for page in pages:
        if isinstance(page, int):
            out.append(page)
        else:
            out.extend(indices[page])
    return out


#: Available colors
COLORS = {
    "bold": "\033[1m",
    "green": "\033[32m",
    "yellow": "\033[33m",
    "red": "\033[31m",
    "reset": "\033[0m",
}


def colorize(text, mapping, colorize=True):
    """Colorize text depending on mapping.

    Parameters
    ----------
    text: str
        Test to colorize
    mapping: dict
        Keys are regular expressions and values are valid :data:`COLORS`.
    colorize: bool
        Whether to colorize or not.

    Return
    ------
    str
    """
    if not colorize or not sys.stdout.isatty():
        return text
    for pattern, color in mapping.items():
        m = re.match(pattern, text)
        if m:
            cc = ""
            for c in color.split("_"):
                cc += COLORS[c]
            return cc + text + COLORS["reset"]
    return text


def flatten(content):
    """Convert an object like a dict to a flat list

    Parameters
    ----------
    content: dict, list, any

    Return
    ------
    list

    """
    out = []
    if isinstance(content, dict):
        for value in content.values():
            out.extend(flatten(value))
    elif isinstance(content, list):
        for value in content:
            out.extend(flatten(value))
    else:
        out.append(content)
    return out


def set_deep_item(dd, value, *keys):
    """Set a deep item in dict

    Parameters
    ----------
    dd: dict
        The dict to modify
    value:
        The value to set
    keys: tuple
        The keys. A key is ignored if set to None.
    """
    keys = [k for k in keys if k is not None]
    for i, key in enumerate(keys):
        if i == len(keys) - 1:
            dd[key] = value
        else:
            dd = dd.setdefault(key, {})
