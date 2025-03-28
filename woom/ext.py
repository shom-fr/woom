#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Woom extensions management
"""
import os
import sys
import importlib


def import_from_path(module_name, file_path):
    """Importing a source file directly

    Source: https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly
    """
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_extensions(workflow_dir):
    """Load woom extensions

    Extensions are python files situated in the :file:`ext` sub-directory of the workflow directory:

    * :file:`jinja_filters.py`: :mod:`jinja` filters.
    * :file:`validator_functions.py`: :mod:`configobj.validate` functions.

    Parameters
    ----------
    workflow_dir: str
        Workflow directory

    Returns
    -------
    list(str)
        The list of loaded extensions

    """
    ext_dir = os.path.join(workflow_dir, "ext")
    exts = []
    if not os.path.exists(ext_dir):
        return exts

    # Jinja filters
    jinja_ext = os.path.join(ext_dir, "jinja_filters.py")
    if os.path.exists(jinja_ext):
        mm = import_from_path("woom.ext.jinja_filters", jinja_ext)
        if hasattr(mm, "JINJA_FILTERS"):
            from .render import JINJA_ENV

            JINJA_ENV.filters.update(mm.JINJA_FILTERS)
            exts.append("jinja_filters")

    # Validator functions
    vf_ext = os.path.join(ext_dir, "validator_functions.py")
    if os.path.exists(vf_ext):
        mm = import_from_path("woom.ext.validator_functions", vf_ext)
        if hasattr(mm, "VALIDATOR_FUNCTIONS"):
            from .conf import VALIDATOR_FUNCTIONS

            VALIDATOR_FUNCTIONS.update(mm.VALIDATOR_FUNCTIONS)
            exts.append("validator_functions")

    return exts
