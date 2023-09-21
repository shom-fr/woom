#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task manager
"""

import os
import shlex
import functools

import configobj

from . import conf as wconf
from . import util as wutil

CFGSPECS_FILE = os.path.join(os.path.dirname(__file__), "tasks.ini")


class TaskError(Exception):
    pass


def format_commandline(line_format, named_arguments=None, subst=None):
    """Format a commandline call

    Parameters
    ----------
    line_format: str
        The commandline call with substitution patterns for named arguments
        that need a value
    named_options: None, dict
        Dictionary the contains the specifications of named arguments
    subst: None, dict
        A dictionary used of substitutions, updated with the named options.

    Return
    ------
    str
    """

    more_subst = {}
    if named_arguments:
        for name, specs in named_arguments.items():
            setops = []
            for value in specs["value"]:
                value = value.strip()
                if value.lower() == "none" or not value:
                    value = None
                if value is not None:
                    value = value.format(**subst)
                    setops.append(
                        specs["format"].format(name=name, value=value)
                    )
                elif specs["required"]:
                    raise TaskError(
                        f"Empty named argument:\nname: {name}\ncommandline: {line_format}"
                    )
            more_subst[name] = " ".join(setops)

    try:
        return line_format.format(**subst, **more_subst)
    except KeyError as e:
        raise TaskError(
            f"Error while performing substitions in commandline: {line_format}\n"
            + f"Please add the '{e.args[0]}' key to the [params] section of the workflow configuration."
        )
    except Exception as e:
        raise Exception(*e.args)


class TaskManager:
    def __init__(self, host):
        self._configs = []
        self._config = configobj.ConfigObj(interpolation=False)
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
        """Get a :class:`Task` instance

        Parameters
        ----------
        name: str
            Known task name
        params: dict
            Dictionary for commandline substitution purpose

        Return
        ------
        Task
        """

        if name not in self._config:
            raise TaskError(f"Invalid task name: {name}")

        # Create instance
        return Task(self._config[name], self.host, params)

    def export_task(self, name, params):
        """Export a task as a dict

        Parameters
        ----------
        name: str
            Known task name
        params: dict
            Dictionary for commandline substitution purpose

        Return
        ------
        dict
            Two keys:

            ``script_content``
                Bash code that must be insert in the submission script
            ``scheduler_options``
                Options that are given to the scheduler when submiting the script
        See also
        --------
        get_task
        """
        return self.get_task(name, params).export()


class Task:
    def __init__(self, taskconfig, host, params):
        self._config = taskconfig
        self._host = host
        self._params = wutil.subst_dict(params)

    @property
    def config(self):
        return self._config

    @property
    def host(self):
        return self._host

    @property
    def params(self):
        return self._params

    @property
    def name(self):
        return self.config.name

    def export_commandline(self):
        """Export the commandline as an bash lines"""
        cc = self.config["commandline"]
        named_arguments = wconf.keep_sections(cc)
        return format_commandline(
            cc["format"],
            named_arguments=named_arguments,
            subst=self.params,
        )

    @functools.cached_property
    def env(self):
        if self.config["submit"]["env"]:
            return self.host.get_env(self.config["submit"]["env"])

    def export_env(self):
        """Export the environment declarations as bash lines"""
        if not self.env:
            return ""
        return str(self.env)

    def export_rundir(self):
        """Export the bash line to move to the running directory"""
        rundir = self.config["submit"]["rundir"]
        if rundir == "current":
            rundir = os.getcwd()
        elif "{" in rundir:
            rundir = rundir.format(**self.params)
        rundir = rundir.strip()
        if rundir:
            return f"mkdir -p {rundir} && cd {rundir}\n\n"
        return ""

    def export_scheduler_options(self):
        if not self.host["scheduler"]:
            return {}
        return {
            "queue": self.host["queues"][self.config["submit"]["queue"]],
            "memory": self.config["submit"]["memory"],
            "time": self.config["submit"]["time"],
            "mail": self.config["submit"]["mail"],
            "log_out": self.config["submit"]["log_out"],
            "extra": self.config["submit"]["extra"].dict(),
        }

    def export(self):
        return {
            "script_content": self.export_env()
            + self.export_rundir()
            + self.export_commandline(),
            "scheduler_options": self.export_scheduler_options(),
        }
