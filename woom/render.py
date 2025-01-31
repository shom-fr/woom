#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Jinja text rendering
"""

from jinja2 import Environment

JINJA_ENV = Environment()


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
    print(opt_name, values)
    calls = []
    for value in values:
        calls.append(format.format(**locals()))
    return " ".join(calls)


register_filters(replicate_option=filter_replicate_option)
