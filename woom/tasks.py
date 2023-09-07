#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task manager
"""

import os
import shlex
import functools

import configobj
import configobj.validate as validate

from . import host as whost
from . import conf as wconf

CFGSPECS_FILE = os.path.join(os.path.basename(__file__), "tasks.ini")


class TaskError(Exception):
    pass


def format_commandline(
    params, executable, positional=None, optional=None, required=None
):
    """Format a commandline call

    Parameters
    ----------
    params: dict
        Parameters that come from the workflow configurations and that are used
        to fill argument values.
    executable: str
        The executable script or program to call
    positional: dict
        The list of positional arguments
    optional: dict
        Optional named arguments with keys that are full option names like "--lon".
    required: list(str)
        List of required arguments, i.e whose value must not be empty or None

    Return
    ------
    str
    """

    def check_arg(name, value):
        value = str(value).strip()
        if value.lower() != "none" or value:
            return value
        if required and name in required:
            raise TaskError(
                f"Empty argument:\nname: {name}\nexecutable: {executable}"
            )
        return value.format(**params)

    args = [executable]
    if positional:
        for name, value in optional.item():
            value = check_arg(name, value)
            args.append(value)
    if optional:
        for name, value in optional.item():
            value = check_arg(name, value)
            args.append(f"{name} {value}")
    return shlex.join(args)


class TaskManager:
    def __init__(self, host):
        self._configs = []
        self._config = configobj.ConfigObj()
        self._host = host

    def load_config(self, cfgfile):
        cfg = wconf.load_cfg(cfgfile, CFGSPECS_FILE)
        self._configs.append(cfg)
        self._postproc_()

    def _postproc_(self):
        if self._configs:
            # Merge
            for cfg in self._configs:
                self._config.merge(cfg)

            # # Apply inheritance
            # for name, content in self._config.items():
            #     if "inherit" in content.scalars:
            #         inherit = content["inherit"]
            #         if inherit in content.sections:

    @property
    def host(self):
        return self._host

    def get_task(self, name, params):
        if name not in self._config:
            raise TaskError(f"Invalid task name: {name}")
        taskconfig = self._config[name]
        params = self.params.copy()
        if name in params:
            params.update(params[name])
            del params[name]
        return Task(taskconfig, params, self.host)

    def __getitem__(self, item):
        return self.get_task(item)

    def export_task(self, name, params):
        """Export a task as a dict

        Parameters
        ----------
        name: str
            Task name
        params: dict

        Return
        ------
        dict
            Two keys:

            ``script_content``
                Bash code that must be insert in the submission script
            ``scheduler_options``
                Options that are given to the scheduler when submiting the script
        """
        self.get_task(name, params).export()


class Task:
    def __init__(self, taskconfig, params, host):
        self._config = taskconfig
        self.params = params
        self._host = host

    @property
    def config(self):
        return self._config

    @property
    def host(self):
        return self._host

    @property
    def name(self):
        return self.config.name

    def export_command_line(self):
        return format_commandline(self.params, **self.config["commandline"])

    @functools.cached_property
    def env(self):
        return self.host.get_env(self.config["submit"]["env"])

    def export_env(self):
        """Export the environment declarations as bash lines"""
        return str(self.env)

    def export_rundir(self):
        """Export the bash line to move to the running directory"""
        rundir = self.config["submit"]["rundir"]
        if rundir == "current":
            rundir = os.getcwd()
        elif "{" in rundir:
            subst = self.params.copy()
            subst.update(self.host.get_dirs())
        rundir = rundir.strip()
        if rundir:
            return f"cd {rundir}\n"
        return ""

    def export_scheduler_options(self):
        if not self.host["scheduler"]:
            return {}
        return {
            "queue": self.host["queues"][self.config["submit"]["queue"]],
            "mem": self.config["submit"]["mem"],
            "extra": self.config["submit"]["extra"].dict(),
        }

    def export(self):
        return {
            "script_content": self.export_env() + self.export_commandline(),
            "scheduler_options": self.export_scheduler_options(),
        }
