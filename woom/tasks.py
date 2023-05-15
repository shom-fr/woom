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

from . import host as jhost

CFGSPECS_FILE = os.path.join(os.path.basename(__file__), "tasks.ini")


class TaskError(Exception):
    pass


def format_commandline(params, executable, positional=None, optional=None, required=None):
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
            raise TaskError(f"Empty argument:\nname: {name}\nexecutable: {executable}")
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
    def __init__(self, params):

        self.validator = validate.Validator()
        self.cfgspecs = configobj.configObj(CFGSPECS_FILE, interpolate=False)
        self.params = params
        self._configs = []
        self._config = configobj.ConfigObj()

    def load(self, cfgfile):
        cfg = configobj.configObj(cfgfile, cfgspecs=self.cfgspecs)
        self.validator.validate(cfg)
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

    def get_task(self, name):
        if name not in self._config:
            raise TaskError(f"Invalid task name: {name}")
        return Task(self._config[name])

    def __getitem__(self, item):
        return self.get_task(item)

    def export_task(self, name):
        """Export a task as a dict

        Parameters
        ----------
        name: str
            Task name

        Return
        ------
        dict
            Two keys:

            ``script_content``
                Bash code that must be insert in the submitted script
            ``scheduler_options``
                Options that are given to the scheduler when submiting the script
        """
        taskconfig = self.config[name]
        params = self.params.copy()
        if name in params:
            params.update(params[name])
            del params[name]
        return self.get_task(taskconfig, params).export()


class Task:
    def __init__(self, taskconfig, params):
        self.params = params
        self.config = taskconfig
        self._env = None

    @functools.cached_property
    def host(self):
        return jhost.get_current_host()

    @property
    def name(self):
        return self.config.name

    def export_command_line(self):
        return format_commandline(self.params, **self.config["commandline"])

    @functools.cached_property
    def env(self):
        return self.host.get_env(self.config["submit"]["env"])

    def export_env(self):
        return str(self.env)

    def export_chdir(self):
        direc = self.host.get_dir(self.config["submit"]["chdir"])
        if "{" in direc:
            direc = direc.format(**self.params)
        if direc:
            return f"cd {direc}\n\n"
        return ""

    def export_scheduler_options(self):
        if not self.host["scheduler"]:
            return {}
        return {
            "queue": self.host["queues"][self.config["submit"]["queue"]],
            "mem": self.config["submit"]["mem"],
        }

    def export(self):
        return {
            "script_content": self.export_env() + self.export_commandline(),
            "scheduler_options": self.export_scheduler_options(),
        }
