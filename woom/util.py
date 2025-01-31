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


class Cycle(collections.UserDict):
    """Useful container for a time cycle that can be used as dict"""

    def __init__(self, begin_date, end_date=None):
        super().__init__()
        self.begin_date = WoomDate(begin_date)
        self.data.update(cycle_begin_date=self.begin_date)
        self.is_interval = end_date is not None
        if not self.is_interval:
            self.end_date = self.duration = None
        else:
            self.end_date = WoomDate(end_date)
            self.duration = self.end_date - self.begin_date
            self.data.update(cycle_end_date=self.end_date, cycle_duration=self.duration)

        # Label
        if self.is_interval:
            self.label = f"{self.begin_date} -> {self.end_date} ({self.duration})"
        else:
            self.label = str(self.begin_date)

        # Token
        fmt = "%Y%m%dT%H%M%S"
        if self.is_interval:
            self.token = f"{self.begin_date:{fmt}}-{self.end_date:{fmt}}"
        else:
            self.token = f"{self.begin_date:{fmt}}"

    def __str__(self):
        return self.token

    def get_params(self, suffix=None):
        """Export a dict of substitution parameters about this cycle"""
        if suffix:
            if not suffix.startswith("_"):
                suffix = "_" + suffix
        else:
            suffix = ""
        params = {
            "cycle_begin_date" + suffix: self["cycle_begin_date"],
            "cycle_label" + suffix: self.label,
            "cycle_token" + suffix: self.token,
        }
        if self.is_interval:
            params.update(
                {
                    "cycle_end_date" + suffix: self["cycle_end_date"],
                    "cycle_duration" + suffix: self["cycle_duration"],
                }
            )
        else:
            params["cycle_date"] = params["cycle_begin_date"]
        return params

    def get_env_vars(self, suffix=None):
        """Export a dict of WOOM env variables about this cycle"""
        params = self.get_params(suffix=suffix)
        return {("WOOM_" + k.upper(), v) for (k, v) in params.items()}


def get_cycles(begin_date, end_date=None, freq=None, ncycle=None, round=None):
    """Get a list of cycles given time specifications

    One cycle is a :class:`Cycle` instance that contains the following keys:

    - ``"cycle_begin_date"``: Begin date [:class:`pandas.Timestamp`]
    - ``"cycle_end_date"`` (optional): End date [:class:`pandas.Timestamp`]
    - ``"cycle_duration"`` (optional): Difference between begin and end [:class:`pandas.Timedelta`]
    """
    if begin_date is None:
        raise WoomError("begin_date must be None to generate cycles")
    begin_date = WoomDate(begin_date, round)

    if end_date:
        end_date = WoomDate(end_date, round)
        if ncycle:
            rundates = pd.date_range(
                start=begin_date,
                end=end_date,
                periods=ncycle + 1,
            )
        elif freq:
            rundates = pd.date_range(
                start=begin_date,
                end=end_date,
                freq=freq,
            )
        else:
            rundates = [
                pd.to_datetime(begin_date),
                pd.to_datetime(end_date),
            ]
    elif ncycle and freq:
        rundates = pd.date_range(
            start=begin_date,
            periods=ncycle + 1,
            freq=freq,
        )
    else:
        rundates = [begin_date]

    # Single date
    if len(rundates) == 1:
        return [Cycle(rundates[0])]

    # A list of time intervals
    cycles = []
    for i, date0 in enumerate(rundates[:-1]):
        date1 = rundates[i + 1]
        cycles.append(Cycle(date0, date1))

    if not cycles:
        raise WoomError(
            f"Unable to generate cycles with these specs: begin_date={begin_date}, end_date={end_date}, freq={freq}, ncycle={ncycle}, round={round}"
        )
    return cycles


class WoomDate(pd.Timestamp):

    re_match_since = re.compile(r"^(years|months|days|hours|minutes|seconds)\s+since\s+(\d+.*)$", re.I).match
    # re_match_add = re.compile(r"^([+\-].+)$").match

    def __new__(cls, date, round=None):
        date = pd.to_datetime(date, utc=True)
        if round:
            date = date.round(round)
        instance = super().__new__(cls, date)
        instance.__class__ = cls
        return instance

    def __format__(self, spec):
        m = self.re_match_since(spec)
        if m:
            units, origin = m.groups()
            return "{:g}".format((self - pd.to_datetime(origin, utc=True)) / pd.to_timedelta(1, units))

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


def make_latest(path, logger=None):
    """Create symbolic link to `path` named "latest"""
    if logger is None:
        logger = logging.getLogger(__name__)
    path = os.path.abspath(path)
    latest = os.path.join(os.path.dirname(path), "latest")
    if os.path.exists(latest):
        if os.path.islink(latest):
            logger.debug(f"Removed link: {latest}")
            os.remove(latest)
        else:
            raise Exception(f"Can't remove since not a link: {latest}")
    logger.debug(f"Create symbolic link: {path} -> {latest}")
    os.symlink(path, latest)
    logger.info(f"Created symbolic link: {path} -> {latest}")
    return latest


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
