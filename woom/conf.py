#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configurations related utilities based on the :mod:`configobj` system
"""
import pathlib
import logging
import pprint

import pandas as pd
import configobj

try:
    import configobj.validate as validate
except ImportError:
    import validate

from . import util as wutil

CACHE = {"cfgspecs": {}}


class ConfigError(Exception):
    pass


def is_path(value):
    if value is None:
        return
    return pathlib.Path(value)


def is_datetime(value, round=None):
    if value is None:
        return
    # utc = validate.is_boolean(utc)
    round = None if str(round).lower() == "none" else round
    return wutil.WoomDate(value, round=round)


def is_timedelta(value):
    if value is None:
        return
    return pd.to_timedelta(value)


VALIDATOR_FUNCTIONS = {
    "path": is_path,
    "datetime": is_datetime,
    "timedelta": is_timedelta,
}


def get_validator():
    """Get a :class:`configobj.validate.Validator` instance"""
    if "validator" not in CACHE:
        CACHE["validator"] = validate.Validator(VALIDATOR_FUNCTIONS)
    return CACHE["validator"]


def get_cfgspecs(cfgspecsfile):
    """Get a configuration specification instance"""
    name = pathlib.Path(cfgspecsfile).stem
    if name not in CACHE["cfgspecs"]:
        CACHE["cfgspecs"][name] = configobj.ConfigObj(cfgspecsfile, interpolation=False, list_values=False)
    return CACHE["cfgspecs"][name]


def load_cfg(cfgfile, cfgspecsfile, list_values=True):
    """Get a validated :class:`configobj.configObj` instance"""
    validator = get_validator()
    cfgspecs = get_cfgspecs(cfgspecsfile)
    cfg = configobj.ConfigObj(
        cfgfile or {},
        configspec=cfgspecs,
        interpolation=False,
        list_values=list_values,
    )
    success = cfg.validate(validator, preserve_errors=True)
    if success is not True:
        msg = f"Error while validating config: {cfgfile}\n"
        msg += pprint.pformat(success)
        logging.getLogger(__name__).error(msg)
        raise ConfigError(msg)
    return cfg


def strip_out_sections(cfg):
    """Remove all section keeping only scalars"""
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
