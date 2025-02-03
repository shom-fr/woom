#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Jinja text rendering
"""
import shlex

from jinja2 import Environment, StrictUndefined

from . import util as wutil

JINJA_ENV = Environment(undefined=StrictUndefined)


def render(template, *args, **kwargs):
    """Render this text with ninja

    Parameters
    ----------
    text: str
        Input template
    *args: tuple
        Objects used for filling
    **kwargs: dict
        Objects used for filling

    Return
    ------
    str
    """
    return JINJA_ENV.from_string(template).render(*args, **kwargs)


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
