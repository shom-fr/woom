#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misc utilities
"""
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
                except KeyError:
                    # print("err")
                    continue
        if not changed:
            break
    else:
        raise WoomError("Detected subsitution sloop")
    return dict_out


def get_cycles(begindate, enddate=None, freq=None, ncycle=None):
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
    if enddate:
        if ncycle:
            rundates = pd.date_range(
                start=begindate,
                end=enddate,
                periods=ncycle + 1,
            )
        elif freq:
            rundates = pd.date_range(
                start=begindate,
                end=enddate,
                freq=freq,
            )
        else:
            rundates = [
                pd.to_datetime(begindate),
                pd.to_datetime(enddate),
            ]
    elif ncycle and freq:
        rundates = pd.date_range(
            start=begindate,
            periods=ncycle + 1,
            freq=freq,
        )
    else:
        raise WoomError(
            "No way to compute cycles since neither 'enddate' nor  'ncycle' + 'freq' options were set."
        )
    cycles = []
    julian_ref = pd.to_datetime("1950-01-01")
    for i, date0 in enumerate(rundates[:-1]):
        date1 = rundates[i + 1]
        cycle = {
            "cycle_begin_date": date0,
            "cycle_end_date": date1,
            "cycle_begin_from_julian": (date0 - julian_ref).days,
            "cycle_end_from_julian": (date1 - julian_ref).days,
            "cycle_duration": date1 - date0,
        }
        cycles.append(cycle)
    return cycles
