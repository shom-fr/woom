#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misc utilities
"""
import re

import pandas as pd

from . import WoomError

RE_MATCH_SINCE = re.compile(
    r"^(years|months|days|hours|minutes|seconds)\s+since\s+(\d+.*)$", re.I
).match


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
                    raise WoomError(
                        "Params substitution error: " + err.args[0]
                    )
        if not changed:
            break
    else:
        raise WoomError("Detected subsitution loop")
    return dict_out


def get_cycles(begin_date, end_date=None, freq=None, ncycle=None, round=None):
    """Get a list of cycles given time specifications

    One cycle is a dictionary that contains the following items:

    - ``"cycle_begin_date"``: Begin date [:class:`pandas.Timestamp`]
    - ``"cycle_end_date"``: End date [:class:`pandas.Timestamp`]
    - ``"cycle_begin_julian"``: Begin date as the number of
      days since 1950 [:class:`int`]
    - ``"cycle_end_julian"``: End date as the number of
      days since 1950 [:class:`int`]
    - ``"duration"``: Difference between begin and end [:class:`pandas.Timedelta`]
    """
    if begin_date is None:
        raise WoomError("begin_date must be a valid date")
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
        return [{"cycle_begin_date": WoomDate(rundates[0])}]

    # A list of time intervals
    cycles = []
    # julian_ref = pd.to_datetime("1950-01-01")
    for i, date0 in enumerate(rundates[:-1]):
        date1 = rundates[i + 1]
        cycle = {
            "cycle_begin_date": WoomDate(date0),
            "cycle_end_date": WoomDate(date1),
            # "cycle_begin_from_julian": (date0 - julian_ref).days,
            # "cycle_end_from_julian": (date1 - julian_ref).days,
            "cycle_duration": date1 - date0,
        }
        cycles.append(cycle)
    return cycles


class WoomDate(pd.Timestamp):
    def __new__(cls, date, round=None):
        date = pd.to_datetime(date)
        if round:
            date = date.round(round)
        instance = super().__new__(cls, date)
        instance.__class__ = cls
        return instance

    def __format__(self, spec):
        m = RE_MATCH_SINCE(spec)
        if m:
            units, origin = m.groups()
            return "{:g}".format(
                (self - pd.to_datetime(origin)) / pd.to_timedelta(1, units)
            )

        return super().__format__(spec)
