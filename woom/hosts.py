#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Host specific configuration
"""
import fnmatch
import functools
import os
import socket

from . import conf as wconf
from . import env as wenv
from . import job as wjob
from .__init__ import WoomError

thisdir = os.path.dirname(__file__)

CFGSPECS_FILE = os.path.join(thisdir, "hosts.ini")

CFG_DEFAULT_FILE = os.path.join(thisdir, "hosts.cfg")


class HostError(WoomError):
    pass


class HostManager:
    def __init__(self):
        self._config = wconf.load_cfg(CFG_DEFAULT_FILE, CFGSPECS_FILE)
        self._host = None

    @property
    def config(self):
        """Configuration as loaded from file :file:`hosts.cfg` (:class:`~configobj.ConfigObj`)"""
        return self._config

    def load_config(self, cfgfile):
        """Load a user configuration file

        .. note:: It is merged with the current one

        Parameters
        ----------
        cfgfile: str
            A valid config file

        Return
        ------
        configobj.ConfigObj
        """
        self._config.merge(wconf.load_cfg(cfgfile, CFGSPECS_FILE))
        return self._config

    def get_host(self, name):
        """Get a :class:`Host` instance from its name"""
        return Host(name, self.config[name])

    def infer_host(self):
        """Infer host and get a :class:`Host` instance"""
        hostname = socket.getfqdn()
        for name, config in self.config.items():
            if name == "local":
                continue
            for pattern in config["patterns"]:
                if fnmatch.fnmatch(hostname, pattern):
                    return self.get_host(name)
        return self.get_host("local")


class Host:
    def __init__(self, name, config):
        self._name = name
        self._config = config
        self._env = None
        self._scheduler = None

    @property
    def name(self):
        """Host name as defined in the configuration (:class:`str`)"""
        return self._name

    def __str__(self):
        return self.name

    @property
    def config(self):
        """Dict configuration of this host as loaded from file :file:`hosts.cfg` (:class:`dict`)"""
        return self._config.dict()

    def __getitem__(self, key):
        return self.config[key]

    @functools.lru_cache
    def get_jobmanager(self):  # , session):
        """Get a :mod:`~woom.job` manager instance

        Returns
        -------
        woom.job.BackgroundJobManager or woom.job.PbsproJobManager or woom.job.SlurmJobManager

        """
        return wjob.BackgroundJobManager.from_scheduler(self.config["scheduler"])  # , session)

    @property
    def module_setup(self):
        """Command to declare the :command:`module` command (:class:`str`)"""
        return self.config["module_setup"]

    @property
    def queues(self):
        """Correspondance between generic and real queue names (:class:`dict`)"""
        return self.config["queues"]

    def get_queue(self, name):
        """Get a queue real name from its generic name

        See also
        --------
        queues
        """
        if name in self.queues:
            return self.queues[name]
        return name

    # def get_dirs(self):
    #     """Get generic directories as dict"""
    #     return self.config["dirs"]

    def get_params(self):
        """Get a context dict for formatting task commandlines with jinja

        In merges the following contents:

        - The ``params`` config section.
        - The ``dirs`` config section with key suffixed with "dir"
          and with the user "~" symbol and environment variables expanded.

        Return
        ------
        dict
        """
        params = {}
        for dname, dval in self.config["dirs"].items():
            if dval:
                dval = os.path.expanduser(os.path.expandvars(dval))
                params[dname + "_dir"] = dval
        return params

    # def get_dir(self, name):
    #     """Get a directory from its generic name

    #     If the value does not contain a path separator, it is interpreted as
    #     an environment variable.
    #     """
    #     if name == "current":
    #         return os.getcwd()
    #     direc = self.config["dirs"][name]
    #     if os.path in direc:
    #         return direc
    #     return "$" + direc

    @functools.cache
    def get_env(self, name):
        """Get a :class:`EnvConfig` instance from a env config name"""

        # Default env
        if name is None:
            return wenv.EnvConfig()

        # Registered?
        if name not in self.config["envs"]:
            available = ', '.join(self.config["envs"])
            raise HostError(f"Invalid environment: {name}. Please choose one of: {available}")
        cfg = self.config["envs"][name]

        # Declare directories as woom env variables
        env_vars = {}
        for dname, dval in self.config["dirs"].items():
            if dval is not None:
                dval = os.path.expanduser(os.path.expandvars(dval))
                env_vars["WOOM_" + dname.upper() + "_DIR"] = dval
        env_vars.update(cfg["vars"]["set"])

        # Get registered env
        return wenv.EnvConfig(
            vars_forward=cfg["vars"]["forward"],
            vars_set=env_vars,
            vars_append=cfg["vars"]["append"],
            vars_prepend=cfg["vars"]["prepend"],
            module_setup=self.config["module_setup"],
            module_use=cfg["modules"]["use"],
            module_load=cfg["modules"]["load"],
            conda_setup=self.config["conda_setup"],
            conda_activate=cfg["conda_activate"],
        )
