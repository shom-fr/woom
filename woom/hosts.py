#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Host specific configuration
"""
import os
import socket
import fnmatch
import functools

import configobj
import configobj.validate as validate

from . import job as jjob
from . import env as jenv

thisdir = os.path.basename(__file__)

CFGSPECS_FILE = os.path.join(thisdir, "hosts.ini")

CFG_DEFAULT_FILE = os.path.join(thisdir, "hosts.cfg")

CACHE = {}


def load_hosts_config(cfgfile=None):

    cfg = configobj.ConfigObj()

    if "validator" not in CACHE:
        CACHE["validator"] = validate.Validator()
    validator = CACHE["validator"]
    if "cfgspecs" not in CACHE:
        CACHE["cfgspecs"] = configobj.ConfigObj(CFGSPECS_FILE)
    cfgspecs = CACHE["cfgspecs"]
    if "default_cfg" not in CACHE:
        CACHE["default_cfg"] = configobj.ConfigObj(CFG_DEFAULT_FILE, configspec=cfgspecs)
        validator.validate(CACHE["default_cfg"])
    cfg.merge(CACHE["default_cfg"])

    if cfgfile:
        cfg.merge(configobj.ConfigObj(cfg, configspec=cfgspecs))
    CACHE["cfg"] = cfg
    return cfg


def get_hosts_config():
    if "cfg" not in CACHE:
        load_hosts_config()
    return CACHE["cfg"]


def infer_host():
    cfg = get_hosts_config()
    hostname = socket.getfqdn()
    for name, config in cfg.items():
        if name == "generic":
            continue
        for pattern in config["patterns"]:
            if fnmatch.fnmatch(hostname, pattern):
                return name
    return "generic"


def get_current_host():
    """Get the current :class:`Host` instance or create it if not existing"""
    if "current_host" not in CACHE:
        name = infer_host()
        CACHE["current_host"] = get_hosts_config()[name]
    return CACHE["current_host"]


def set_current_host(host):
    """Set the current host

    Parameters
    ----------
    host: str, Host
        If a string, it is interpreted as a host name and a :class:`Host` instance is created

    Return
    ------
    Host
    """
    if isinstance(host, str):
        config = get_hosts_config()[host]
        host = Host(config)
    CACHE["current_host"] = host
    return host


class Host:
    def __init__(self, config):
        self.config = config
        self._env = None

    @property
    def name(self):
        return self.config.name

    @functools.cached_property
    def scheduler(self):
        """Scheduler instance"""
        return jjob.BasicScheduler.from_type(self.config.scheduler)

    @property
    def module_setup(self):
        return self.config["module_setup"]

    @property
    def queues(self):
        return self.config["queues"]

    def get_queue(self, name):
        """Get a queue real name from its generic name"""
        if name in self.queues:
            return self.queues[name]
        return name

    def get_dir(self, name):
        """Get a directory from its generic name

        If the value does not contain a path separator, it is interpreted as
        an environment variable.
        """
        direc = self.config["dirs"][name]
        if os.path in direc:
            return direc
        return "$" + direc

    @functools.cache
    def get_env(self, name):
        """Get a :class:`EnvConfig` instance from a env config name"""
        cfg = self.config["envs"][name]
        return jenv.EnvConfig(
            module_setup=cfg["module_setup"],
            module_use=cfg["modules"]["use"],
            module_load=cfg["modules"]["load"],
            vars_forward=cfg["vars"]["forward"],
            vars_set=cfg["vars"]["set"],
            vars_append=cfg["vars"]["append"],
            vars_prepend=cfg["vars"]["prepend"],
        )
