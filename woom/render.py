#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Jinja text rendering
"""
import os
import shlex

from jinja2 import Environment, PackageLoader, StrictUndefined, Undefined

from . import util as wutil

#: :class:`jinja2.Environment` used to render woom commandline templates
JINJA_ENV = Environment(loader=PackageLoader("woom"), undefined=StrictUndefined, trim_blocks=True)


def render(template, params, strict=True, nested=True):
    """Render this text with ninja

    Note
    ----
    Rendering is performed through a recursive process until the final
    string does not change. This allows passing parameters that contains jinja patterns.

    Parameters
    ----------
    text: str, jinja2.Template
        Input template
    params: dict
        Objects used for filling

    Return
    ------
    str
    """
    if not strict:
        JINJA_ENV.undefined = Undefined
    prev = template
    while True:
        if isinstance(prev, str):
            tpl = JINJA_ENV.from_string(prev)
        else:
            tpl = prev
        curr = tpl.render(params)
        if nested and (not isinstance(prev, str) or curr != prev):
            prev = curr
        else:
            break
    if not strict:
        JINJA_ENV.undefined = StrictUndefined
    return curr


def filter_replicate_option(values, opt_name, format="{opt_name}={value}"):
    """Replicate a command line option with values

    Parameters
    ----------
    values: list
        Values to distribute
    opt_name: str
        Option name, typically startwith with a dash '-'
    format: str
        How to format the option

    Return
    ------
    str

    Example
    -------
    >>> filter_replicate_option(['uo', 'vo'], '--var')
    '--var=uo --var=vo'
    """
    calls = []
    if not isinstance(values, list):
        values = [values]
    for value in values:
        value = shlex.quote(value)
        calls.append(format.format(**locals()))
    return " ".join(calls)


def filter_strftime(date, format):
    """Create a :class:`~woom.util.WoomDate` and format it

    Parameters
    ----------
    date: str, pandas.Timestamp, woom.util.WoomDate
        Date to initialize a :class:`~woom.util.WoomDate`
    format: str
        Iso format

    Return
    ------
    str

    Example
    -------
    >>> filter_strftime("2025-02-01T02:12", "%Y-%m")
    "2025-02"

    See also
    --------
    woom.util.WoomDate

    """
    return wutil.WoomDate(date).strftime(format)


def filter_as_env_str(value):
    """Convert to environment variable string

    Parameters
    ----------
    value:
        Vaule to convert as string

    Return
    ------
    str

    Example
    -------
    >>> filter_as_env_str(2.5)
    '2.5'
    >>> filter_as_env_str(["uo", "vo"])
    'uo:vo'
    """
    if isinstance(value, (list, tuple, set)):
        return os.pathsep.join([str(v) for v in value])
    return str(value)


#: Default woom jinja filters
JINJA_FILTERS = dict(
    replicate_option=filter_replicate_option, strftime=filter_strftime, as_str_env=filter_as_env_str
)

JINJA_ENV.filters.update(JINJA_FILTERS)
