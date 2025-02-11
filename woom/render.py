#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Jinja text rendering
"""
import shlex

from jinja2 import Environment, StrictUndefined

from . import util as wutil

#: :class:`jinja2.Environment` used to render woom commandline templates
JINJA_ENV = Environment(undefined=StrictUndefined)


def render(template, params):
    """Render this text with ninja

    Note
    ----
    Rendering is performed through a recursive process to until the final
    string does not change. This allows passing parameters that contain jinja patterns.

    Parameters
    ----------
    text: str
        Input template
    params: dict
        Objects used for filling

    Return
    ------
    str
    """
    prev = template
    while True:
        curr = JINJA_ENV.from_string(prev).render(**params)
        if curr != prev:
            prev = curr
        else:
            return curr


def register_filters(**kwargs):
    """Register filter functions in the current environment"""
    JINJA_ENV.filters.update(**kwargs)


def filter_replicate_option(values, opt_name, format="{opt_name}={value}"):
    """Replicate a command line option with values"""
    calls = []
    if not isinstance(values, list):
        values = [values]
    for value in values:
        value = shlex.quote(value)
        calls.append(format.format(**locals()))
    return " ".join(calls)


def filter_strftime(date, format):
    """Create a :class:`~woom.util.WoomDate` and format it"""
    return wutil.WoomDate(date).strftime(format)


register_filters(replicate_option=filter_replicate_option, strftime=filter_strftime)
