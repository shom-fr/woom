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
    filepath: str
        File path
    dry: bool
        Fake mode. Do not create the directory
    logger: logging.Logger
        To inform that we create the directory, even in dry mode

    Return
    ------
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
    def default(self, obj):
        if isinstance(obj, collections.UserDict):
            return dict(obj)
        if hasattr(obj, "pid") or isinstance(obj, subprocess.Popen):
            return obj.pid
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)


def params2env_vars(params=None, select=None, **extra_params):
    """Convert a dict of parameters to env vars start whose name starts with ``'WOOM_'``"""
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
        env_vars["WOOM_" + key.upper()] = str(value)
    return env_vars


def pages2ints(pages, n):
    """Convert a list of 1-based integers and zero-based slices to a pure list of one-based integers"""
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
