#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misc utilities
"""
import re
import os
import logging
import json
import collections
import subprocess

import pandas as pd

from . import WoomError


def subst_dict(dict_in, dict_subst=None, maxiter=None):
    """Perform recursive string substitutions in a dictionary

    .. note:: It does not go into sub-directories

    Parameters
    ----------
    dict_in: dict
    maxiter: None, int
        Maximum number of iteration to prevent loops.
        It defaults to ``len(dict_in)*10``

    Return
    ------
    dict
    """
    if maxiter is None:
        maxiter = len(dict_in) * 10
    dict_out = dict_in.copy()
    if dict_subst is None:
        dict_subst = {}
    else:
        dict_subst = dict_subst.copy()
    for i in range(maxiter):
        dict_subst_full = dict_subst.copy()
        dict_subst_full.update(dict_out)
        changed = False
        for key, val in dict_out.items():
            if isinstance(val, str):
                try:
                    val_new = val.format(**dict_subst_full)
                    if val != val_new:
                        changed = True
                        # print((key, val, val_new))
                    dict_out[key] = dict_subst[key] = val_new
                except KeyError as err:
                    raise WoomError("Params substitution error: " + err.args[0])
        if not changed:
            break
    else:
        raise WoomError("Detected subsitution loop")
    return dict_out


class WoomDate(pd.Timestamp):
    re_match_since = re.compile(
        r"^(years|months|days|hours|minutes|seconds)\s+since\s+(\d+.*)$", re.I
    ).match
    # re_match_add = re.compile(r"^([+\-].+)$").match

    def __new__(cls, date, round=None):
        if isinstance(date, str) and date in ["now", "today"]:
            utc = True
        else:
            utc = False
        date = pd.to_datetime(date, utc=utc)
        if utc:
            date = date.tz_localize(None)
        if round:
            date = date.round(round)
        instance = super().__new__(cls, date)
        instance.__class__ = cls
        return instance

    def __format__(self, spec):
        m = self.re_match_since(spec)
        if m:
            units, origin = m.groups()
            return "{:g}".format(
                (self - pd.to_datetime(origin, utc=True)) / pd.to_timedelta(1, units)
            )

        return super().__format__(spec)

    def add(self, *args, **kwargs):
        """Add time delta"""
        return self + pd.to_timedelta(*args, **kwargs)


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
        if isinstance(obj, subprocess.Popen):
            return obj.pid
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)


def params2env_vars(params=None, **extra_params):
    """Convert a dict of parameters to env vars start whose name starts with 'WOOM\_'"""
    if params is None:
        params = extra_params
    else:
        params = params.copy()
        params.update(extra_params)
    env_vars = {}
    for key, value in params.items():
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
