#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configurations related utilities based on the :mod:`configobj` system
"""
import logging
import pathlib
import pprint
import re

import configobj
import pandas as pd

try:
    import configobj.validate as validate
except ImportError:
    import validate

from . import util as wutil
from .__init__ import WoomError

CACHE = {"cfgspecs": {}}


class WoomConfigError(WoomError):
    pass


def is_path(value):
    """Convert to :class:`pathlib.Path`"""
    if value is None:
        return
    try:
        return pathlib.Path(value)
    except Exception as e:
        raise WoomConfigError("Can't convert config value to path: " + e.args[0])


def is_datetime(value, round=None):
    """Convert to :class:`pandas.Timestamp`"""
    if value is None:
        return
    # utc = validate.is_boolean(utc)
    round = None if str(round).lower() == "none" else round
    try:
        return wutil.WoomDate(value, round=round)
    except Exception as e:
        raise WoomConfigError("Can't convert config value to datetime: " + e.args[0])


def is_timedelta(value):
    """Convert to :class:`pandas.Timedelta`"""
    if value is None:
        return
    try:
        return pd.to_timedelta(value)
    except Exception as e:
        raise WoomConfigError("Can't convert config value to timedelta: " + e.args[0])


def is_pages(values):
    """Convert a one-based page-like selection

    .. note:: Multi-selections are converted to zero-based slices

    Parameters
    ----------
    values: str, list(str)

    Example
    ------
    .. ipython:: python
        :okwarning:

        @suppress
        from woom.conf import is_pages
        is_pages("4,5,7-")
    """

    if values is None:
        return
    if not isinstance(values, list):
        values = [values]
    re_split_c = re.compile(r"\s*,\s*").split
    sels = []
    for v in values:
        sels.extend(re_split_c(v))
    out = []
    re_split_t = re.compile(r"\s*\-\s*").split
    for sel in sels:
        if isinstance(sel, str) and "-" in sel:
            ssel = [
                (int(s) if i > 0 else int(s) - 1) if s != "" else None for i, s in enumerate(re_split_t(sel))
            ]
            out.append(slice(*ssel))
        else:
            out.append(int(sel))
    return out


#: Default woom validator fonctions
VALIDATOR_FUNCTIONS = {
    "path": is_path,
    "datetime": is_datetime,
    "timedelta": is_timedelta,
    "pages": is_pages,
}


def get_validator():
    """Get a :class:`configobj.validate.Validator` instance"""
    return validate.Validator(VALIDATOR_FUNCTIONS)
    # if "validator" not in CACHE:
    #     CACHE["validator"] = validate.Validator(VALIDATOR_FUNCTIONS)
    # return CACHE["validator"]


def get_cfgspecs(cfgspecsfiles):
    """Get a configuration specification instance from a list of files"""
    cfgspecs = None
    for cfgspecsfile in cfgspecsfiles if isinstance(cfgspecsfiles, list) else [cfgspecsfiles]:
        this_cfgspecs = configobj.ConfigObj(cfgspecsfile, interpolation=False, list_values=False)
        if cfgspecs is None:
            cfgspecs = this_cfgspecs
        else:
            cfgspecs.merge(this_cfgspecs)
    return cfgspecs
    # if name not in CACHE["cfgspecs"]:
    #     CACHE["cfgspecs"][name] =
    # return CACHE["cfgspecs"][name]


def load_cfg(cfgfile, cfgspecsfiles, list_values=True, interpolation=True):
    """Get a validated :class:`configobj.configObj` instance"""
    validator = get_validator()
    cfgspecs = get_cfgspecs(cfgspecsfiles)
    cfg = configobj.ConfigObj(
        cfgfile or {},
        configspec=cfgspecs,
        interpolation=interpolation,
        list_values=list_values,
    )
    success = cfg.validate(validator, preserve_errors=True)
    if success is not True:
        msg = f"Error while validating config: {cfgfile}\n"
        msg += pprint.pformat(success)
        logging.getLogger(__name__).error(msg)
        raise WoomConfigError(msg)
    return cfg


def strip_out_sections(cfg):
    """Remove all sections keeping only scalars"""
    cfgo = configobj.ConfigObj(cfg, interpolation=cfg.main.interpolation)
    for key in cfg.sections:
        del cfgo[key]
    return cfgo


def keep_sections(cfg):
    """Only keep section"""
    cfgo = configobj.ConfigObj(cfg, interpolation=cfg.main.interpolation)
    for key in cfg.scalars:
        del cfgo[key]
    return cfgo


def merge_args_with_config(cfg, args, names, prefix=None):
    """Merge parser arguments with configuration items

    .. note:: The configuration is modified in place

    Parameters
    ----------
    cfg: configobj.ConfigObj
    args: argparse.Namespace
    names: list(str)
    prefix: str
        String to prepend to names in `args`
    """
    prefix = prefix or ""
    for name in names:
        value = getattr(args, prefix + name, None)
        if value is not None:
            cfg[name] = value


def inherit_cfg(cfg, inherit_from):
    """Inherit content from another config"""
    for key, val in list(inherit_from.items()):
        if key not in cfg or cfg[key] is None:
            cfg[key] = val
        elif isinstance(cfg[key], dict) and isinstance(val, dict):
            inherit_cfg(cfg[key], val)
