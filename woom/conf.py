#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configurations related utilities based on the :mod:`configobj` system
"""
import pathlib

import pandas as pd
import configobj
import configobj.validate as validate


CACHE = {"cfgspecs": {}}


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


def get_validator(self):
    """Get a :class:`configobj.validate.Validator` instance"""
    if "validator" not in CACHE:
        CACHE["validator"] = validate.Validator(VALIDATOR_FUNCTIONS)
    return CACHE["validator"]


def get_cfgspecs(cfgspecsfile):
    """Get a configuration specification instance"""
    name = pathlib.Path(cfgspecsfile).stem
    if name not in CACHE["cfgspecs"]:
        CACHE["cfgspecs"][name] = configobj.configObj(
            cfgspecsfile, interpolate=False
        )
    return CACHE["cfgspecs"][name]


def load_cfg(cfgfile, cfgspecsfile):
    """Get a validated :class:`configobj.configObj` instance"""
    validator = get_validator()
    cfgspecs = get_cfgspecs(cfgspecsfile)
    cfg = configobj.configObj(
        cfgfile or {}, cfgspecs=cfgspecs, interpolate=False
    )
    validator.validate(cfg)
    return cfg
