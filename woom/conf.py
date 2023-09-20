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
import configobj.validate as validate


CACHE = {"cfgspecs": {}}


class ConfigError(Exception):
    pass


def is_path(value):
    if value is None:
        return
    return pathlib.Path(value)


def is_datetime(value):
    if value is None:
        return
    return pd.to_datetime(value).to_pydatetime()


def is_timedelta(value):
    if value is None:
        return
    return pd.to_timedelta(value).to_pytimedelta()


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
        CACHE["cfgspecs"][name] = configobj.ConfigObj(
            cfgspecsfile, interpolation=False, list_values=False
        )
    return CACHE["cfgspecs"][name]


def load_cfg(cfgfile, cfgspecsfile):
    """Get a validated :class:`configobj.configObj` instance"""
    validator = get_validator()
    cfgspecs = get_cfgspecs(cfgspecsfile)
    cfg = configobj.ConfigObj(
        cfgfile or {},
        configspec=cfgspecs,
        interpolation=False,
        list_values=False,
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
