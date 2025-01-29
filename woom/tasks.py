#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task manager
"""

import os
import re
import functools

import configobj

from .__init__ import WoomError
from . import conf as wconf
from . import util as wutil

CFGSPECS_FILE = os.path.join(os.path.dirname(__file__), "tasks.ini")
RE_SPLIT_COMMAS = re.compile(r"\s*,\s*").split


class TaskError(WoomError):
    pass


class TaskTree:
    """Postprocess configuration to build a task tree"""

    def __init__(self, stages, groups=None):
        """
        Parameters
        ----------
        stages: :class:`configobj.Section`
        groups: None, :class:`configobj.Section`
            Group of tasks that can be used in stages
        """
        self._stages = configobj.ConfigObj(stages)
        self._groups = configobj.ConfigObj(groups)

    @functools.cache
    def to_dict(self):
        all_tasks = []
        tt = {}
        for stage in self._stages.sections:  # prolog, tokens, epilog
            tt[stage] = {}

            # Loop on sub-stages
            for substage, tasks_line in self._stages[stage].items():  # fetch=task1,group1,...
                tasks = RE_SPLIT_COMMAS(tasks_line)
                tt[stage][substage] = tasks

                # Loop on parallel tasks
                for i in range(len(tasks)):
                    task = tasks[i]
                    if task in self._groups:
                        tasks[i] = RE_SPLIT_COMMAS(self._groups[task])
                    else:
                        tasks[i] = [task]  # as a single element group

                    # Check unicity
                    for task in tasks[i]:
                        if task in all_tasks:
                            raise TaskError(f"Duplicate tasks not allowed: {task}")
                        all_tasks.append(task)
        return tt

    def __str__(self):
        dd = self.to_dict()
        ss = ""
        for stage, scontent in dd.items():
            ss += f"{stage}:\n"
            for substage, sscontent in scontent.items():
                ss += f"    - {substage}: "
                tasks = []
                for gt in sscontent:
                    if len(gt) == 1:
                        tasks.append(gt[0])
                    else:
                        tasks.append("[" + " -> ".join(gt) + "]")
                ss += " // ".join(tasks) + "\n"
            # ss += "\n"
        return ss.strip("\n")


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
                    setops.append(specs["format"].format(name=name, value=value))
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
    def __init__(self, host):  # , session):
        self._configs = []
        self._config = configobj.ConfigObj(interpolation=False)
        self._host = host
        # self._session = session
        # os.environ["WOOM_SESSION_DIR"] = str(session.path)

    def load_config(self, cfgfile):
        cfg = wconf.load_cfg(cfgfile, CFGSPECS_FILE)
        self._configs.append(cfg)
        self._postproc_()

    def _postproc_(self):
        if self._configs:
            # Merge
            for cfg in self._configs:
                self._config.merge(cfg)

            # Apply inheritance
            not_complete = True
            while not_complete:
                not_complete = False
                for name, content in self._config.items():
                    if "inherit" in content.scalars:
                        inherit = content["inherit"]
                        if inherit:
                            if inherit in self._config:
                                wconf.inherit_cfg(self._config[name], self._config[inherit])
                                if self._config[name]["inherit"] == inherit:
                                    self._config[name]["inherit"] = None
                                else:
                                    not_complete = True
                            else:
                                raise TaskError(f"Wrong task name to inherit from: {inherit}")

    @property
    def host(self):
        return self._host

    # @property
    # def session(self):
    # return self._session

    def get_task(self, name, params, token=None):
        """Get a :class:`Task` instance

        Parameters
        ----------
        name: str
            Known task name
        params: dict
            Dictionary for commandline substitution purpose
        token: str, None
            A signature that helps defining this task

        Return
        ------
        Task
        """

        if name not in self._config:
            raise TaskError(f"Invalid task name: {name}")

        # Create instance
        return Task(self._config[name], self.host, params, token)


class Task:
    def __init__(self, taskconfig, host, params, token=None):
        self._config = taskconfig
        self._host = host
        self._params = wutil.subst_dict(params)
        self._token = token

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

    @property
    def token(self):
        return self._token

    def export_commandline(self):
        """Export the commandline as an bash lines"""
        cc = self.config["content"]["commandline"]
        named_arguments = wconf.keep_sections(cc)
        return (
            "\n# Run the commandline(s)\n"
            + format_commandline(
                cc["format"],
                named_arguments=named_arguments,
                subst=self.params,
            )
            + "\n"
        )

    @functools.cached_property
    def env(self):
        """Instance of :class:`woom.env.Env` specific to this task"""

        # Get env from name, possibly empty
        env = self.host.get_env(self.config["content"]["env"]["name"]).copy()

        # Add task env variables with substitutions
        cfgvars = self.config["content"]["env"]["vars"]
        for action in "set", "prepend", "append":
            for name, value in cfgvars[action].items():
                value = value.format(**self.params)
                cfgvars[action][name] = value
        if cfgvars["set"]:
            env.vars_set.update(cfgvars["set"])
        if cfgvars["prepend"]:
            env.vars_prepend.update(cfgvars["prepend"])
        if cfgvars["append"]:
            env.vars_append.update(cfgvars["append"])

        # Add woom variables
        env.vars_set.update(WOOM_TASK_NAME=self.name)
        env.vars_forward.extend(["WOOM_WORKFLOW_DIR"])  # , "WOOM_SESSION_DIR"])
        return env

    def export_env(self):
        """Export the environment declarations as bash lines"""
        # self.env.vars_set.update(WOOM_TASK_TOKEN=str(token))
        return str(self.env)

    def export_rundir(self):
        """Export the bash line to move to the running directory"""
        rundir = self.config["content"]["rundir"]
        if rundir is None:
            return ""
        if rundir == "current":
            rundir = os.getcwd()
        elif "{" in rundir:
            rundir = rundir.format(**self.params)
        rundir = rundir.strip()
        if rundir:
            return f"\n# Got to run dir\nmkdir -p {rundir} && cd {rundir}\n\n"
        return ""

    # def export_epilog(self):
    #     return "mkdir -p $WOOM_SESSION_DIR/

    def export_scheduler_options(self):
        if not self.host["scheduler"]:
            return {}
        opts = {
            "memory": self.config["submit"]["memory"],
            "time": self.config["submit"]["time"],
            "mail": self.config["submit"]["mail"],
            "log_out": self.config["submit"]["log_out"],
            "extra": self.config["submit"]["extra"].dict(),
        }
        if self.config["submit"]["queue"]:
            opts["queue"] = self.host["queues"][self.config["submit"]["queue"]]
        return opts

    def export_prolog(self):
        prolog = "\n# Prolog\n"
        prolog += f'trap "false" {self.config["content"]["trap"]}\n\n'
        return prolog

    def export_epilog(self):
        epilog = "\n# Epilog\n"
        epilog += "echo $? > $WOOM_SUBMISSION_DIR/job.status\n"
        return epilog

    def export(self):
        return {
            "script_content": "#!/bin/bash\n\n"
            + self.export_prolog()
            + self.export_env()
            + self.export_rundir()
            + self.export_commandline()
            + self.export_epilog()
            + "\n",
            "scheduler_options": self.export_scheduler_options(),
        }
