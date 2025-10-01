#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Iteration utilities for date cycles and ensembles
"""

import math

import pandas as pd

from . import WoomError
from . import util as wutil

# %% Cycles


class Cycle:
    """Container for a time cycle"""

    def __init__(self, begin_date, end_date=None):
        #: Begin date (:class:`~woom.util.WoomDate`)
        self.begin_date = wutil.WoomDate(begin_date)
        #: Same as :attr:`begin_date`
        self.date = self.begin_date
        #: Whether it is an interval or a single date (:class:`bool`)
        self.is_interval = end_date is not None
        #: Whether it is the first cycle  (:class:`bool`)
        self.is_first = False
        #: Whether it is the last cycle  (:class:`bool`)
        self.is_last = False
        if not self.is_interval:
            self.end_date = self.duration = None
        else:
            #: End date (:class:`~woom.util.WoomDate` or None)
            #: defaults to None
            self.end_date = wutil.WoomDate(end_date)
            #: Interval duration (:class:`~pandas.Timedelta` or None)
            self.duration = self.end_date - self.begin_date

        # Label
        if self.is_interval:
            self.label = f"{self.begin_date.isoformat()} -> {self.end_date.isoformat()} ({self.duration})"
        else:
            #: String used for for printing and based on the ISO 8601 format (:class:`str`)
            self.label = self.begin_date.isoformat()

        # Token
        if self.is_interval:
            self.token = f"{self.begin_date.isoformat()}-{self.end_date.isoformat()}"
        else:
            #: String used in file and directory names and based on the ISO 8601 format (:class:`str`)
            self.token = f"{self.begin_date.isoformat()}"

        #: Next cycle (:class:`Cycle` or None)
        self.next = None
        #: Previous cycle (:class:`Cycle` or None)
        self.prev = None

    def __str__(self):
        return self.token

    def __repr__(self):
        ss = f"<Cycle({self.begin_date}, {self.end_date})>\n"
        for attr in "duration", "date", "label", "token", "is_first", "is_last", "prev", "next":
            ss += " {}: {}\n".format(attr, getattr(self, attr))
        return ss

    def describe(self):
        return self.__repr__()

    def __hash__(self):
        return hash(self.token)

    def get_params(self, suffix=None):
        """Export a dict of substitution parameters about this cycle"""
        if suffix:
            if not suffix.startswith("_"):
                suffix = "_" + suffix
        else:
            suffix = ""
        params = {
            "cycle" + suffix: self,
            "cycle_begin_date" + suffix: self.begin_date,
            "cycle_label" + suffix: self.label,
            "cycle_token" + suffix: self.token,
        }
        if self.is_interval:
            params.update(
                {
                    "cycle_end_date" + suffix: self.end_date,
                    "cycle_duration" + suffix: self.duration,
                }
            )
        else:
            params["cycle_date" + suffix] = params["cycle_begin_date" + suffix]
        params["cycle_is_first" + suffix] = self.is_first
        params["cycle_is_last" + suffix] = self.is_last
        params["cycle_next" + suffix] = self.next
        params["cycle_prev" + suffix] = self.prev
        return params

    def get_env_vars(self, suffix=None):
        """Export a dict of WOOM environment variables about this cycle"""
        params = self.get_params(suffix=suffix)
        return wutil.params2env_vars(params)


def gen_cycles(begin_date, end_date=None, freq=None, ncycles=None, round=None, as_intervals=True):
    """Get a list of :class:`Cycle` instances given time specifications

    The first cycle has the :attr:`Cycle.is_first` attribute set to True.
    The last cycle has the :attr:`Cycle.is_last` attribute set to True.
    The cycles are related with one another thanks to the
    :attr:`Cycle.prev` and  :attr:`Cycle.next` attributes.

    Parameters
    ----------
    begin_date: date-like
        First date
    end_date: date_like, None
        Last date
    freq: freq-like, None
        Difference of time between to dates
    ncycles: int, None
        Number of cycles. This parameters takes precedence over `freq`.
    round: freq_like, None
        Round dates to this precision
    as_intervals: bool
        Consider dates as independant dates or intervals.
        When set to True, ``[date0, date1, date2]`` becomes
        ``[Cycle(date0, date1), Cycle(date1, date2)]``,
        else
        ``[Cycle(date0), Cycle(date1), Cycle(date2)]``.
    """
    if begin_date is None:
        raise WoomError("begin_date must be None to generate cycles")
    begin_date = wutil.WoomDate(begin_date, round)

    if end_date:
        end_date = wutil.WoomDate(end_date, round)
        if ncycles:
            rundates = pd.date_range(
                start=begin_date,
                end=end_date,
                periods=ncycles + 1,
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
    elif ncycles and freq:
        rundates = pd.date_range(
            start=begin_date,
            periods=ncycles + 1,
            freq=freq,
        )
    else:
        rundates = [begin_date]

    # Single date
    if len(rundates) == 1:
        return [Cycle(rundates[0])]

    # A list of time intervals
    if as_intervals:
        cycles = []
        for i, date0 in enumerate(rundates[:-1]):
            date1 = rundates[i + 1]
            cycles.append(Cycle(date0, date1))
    else:
        cycles = [Cycle(date) for date in rundates]

    if not cycles:
        raise WoomError(
            "Unable to generate cycles with these specs: "
            f"begin_date={begin_date}, end_date={end_date}, freq={freq}, ncycle={ncycles}, round={round}"
        )

    cycles[0].is_first = True
    cycles[-1].is_last = True
    for i in range(0, len(cycles)):
        if i != 0:
            cycles[i].prev = cycles[i - 1]
        if i != len(cycles) - 1:
            cycles[i].next = cycles[i + 1]

    return cycles


# %% Ensembles


class Member:
    """Container for an ensemble member"""

    def __init__(self, member_id, nmembers):
        #: Member id starting from 1 (:class:`int`)
        self.id = member_id
        #: Total number of members in the esemble  (:class:`int`)
        self.nmembers = nmembers
        self._ndigits = int(math.log10(self.nmembers)) + 1
        self._props = set()

    def __str__(self):
        return str(self.id)

    def set_prop(self, name, value):
        """Set a property"""
        setattr(self, name, value)
        self._props.update({name})

    @property
    def props(self):
        """Properties of this member (:class:`dict`)"""
        return dict((name, getattr(self, name)) for name in self._props)

    @property
    def label(self):
        """String like 'member12' (:class:`str`)"""
        return f"member{self.id:0{self._ndigits}}"

    @property
    def rank(self):
        """String like '012/120' (:class:`str`)"""
        return f"{self.id:0{self._ndigits}}/{self.nmembers}"

    @property
    def params(self):
        """Contains this instance, :attr:`nmembers` and all :attr:`properties <props>` (:class:`dict`)

        It is used for string substitutions
        """
        params = {"member": self, "nmembers": self.nmembers}
        params.update(self.props)
        return params

    @property
    def env_vars(self):
        """Conversion of :attr:`params` to a dict of environment variables  (:class:`dict`)"""
        return wutil.params2env_vars(self.params)


def gen_ensemble(nmembers, skip=None, **iters):
    """Generate a list of :class:`Member` objects"""
    # nmembers from iters
    if nmembers is None:
        if iters:
            nmembers = min([len(v) for v in iters.values()])
        else:
            nmembers = 0

    # Skip some members
    if skip:
        skip = wutil.pages2ints(skip, nmembers)

    # loop on members
    members = []
    for member_id in range(1, nmembers + 1):
        if skip and member_id in skip:
            continue
        member = Member(member_id, nmembers)
        for attr, values in iters.items():
            nvalues = len(values)
            if nvalues != nmembers:
                raise WoomError(
                    f"Ensemble iterator names '{attr}' must have a length of {nmembers}, not {nvalues}!"
                )
            member.set_prop(attr, values[member_id - 1])
        members.append(member)
    return members
