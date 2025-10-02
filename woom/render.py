#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Jinja text rendering
"""
import os
import shlex

from jinja2 import (
    BaseLoader,
    ChoiceLoader,
    Environment,
    FileSystemLoader,
    PackageLoader,
    StrictUndefined,
    TemplateNotFound,
    Undefined,
)

from . import util as wutil

#: :class:`jinja2.Environment` used to render woom commandline templates
JINJA_ENV = Environment(loader=PackageLoader("woom"), undefined=StrictUndefined, trim_blocks=True)


def setup_template_loader(workflow_dir):
    """Setup Jinja loader to support user template extensions

    This allows users to extend base templates using Jinja inheritance.
    User templates are searched in: :file:`{workflow_dir}/templates/`

    .. warning:: To inherit from the default template,
        prefix the template name with a "!".

    Parameters
    ----------
    workflow_dir: str
        Path to the workflow directory

    Example
    -------
    User can create :file:`{workflow_dir}/templates/job.sh` with:

    .. code-block:: jinja

        {% extends "!job.sh" %}
        {% block header %}
        {{ super() }}
        # Custom header additions
        echo "Starting custom workflow"
        {% endblock %}

    """
    user_template_dir = os.path.join(workflow_dir, "templates")
    JINJA_ENV.loader = WoomLoader(workflow_dir)
    return user_template_dir


class WoomLoader(BaseLoader):
    """Jinja loader that searches for use templates

    Base template is laodable with a "!" prefix.
    """

    def __init__(self, workflow_dir):
        user_template_dir = os.path.join(workflow_dir, "templates")
        if os.path.exists(user_template_dir):
            self._woom_user_loader = FileSystemLoader(user_template_dir)
        else:
            self._woom_user_loader = None
        self._woom_package_loader = PackageLoader("woom")

    def get_source(self, environment, template):
        loaders = [self._woom_package_loader]
        if not template.startswith('!') and self._woom_user_loader:
            loaders.insert(0, self._woom_user_loader)
        template = template.lstrip("!")
        for loader in loaders:
            try:
                return loader.get_source(environment, template)
            except TemplateNotFound:
                pass
        raise TemplateNotFound(f"Templates {template} not found")


def render(template, params, strict=True, nested=True):
    """Render this text with Jinja

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
