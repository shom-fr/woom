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
from . import render as wrender

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
                tasks = tasks_line  # RE_SPLIT_COMMAS(tasks_line)
                tt[stage][substage] = tasks

                # Loop on parallel tasks
                for i in range(len(tasks)):
                    task = tasks[i]
                    if task in self._groups:
                        tasks[i] = self._groups[task]  # RE_SPLIT_COMMAS(self._groups[task])
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
            if not scontent:
                continue
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
        if not ss:
            ss = "Empty workflow!"
        return ss.strip("\n")


class TaskManager:
    def __init__(self, host):  # , session):
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

    def get_task(self, name):
        """Get a :class:`Task` instance

        Parameters
        ----------
        name: str
            Known task name

        Return
        ------
        Task
        """

        if name not in self._config:
            raise TaskError(f"Invalid task name: {name}")

        # Create instance
        return Task(self._config[name], self.host)


class Task:
    def __init__(self, taskconfig, host):
        self._config = taskconfig
        self._host = host

    @property
    def config(self):
        """The task configuration as loaded from the :file:`tasks.cfg` (:class:`~configobj.ConfigObj`)"""
        return self._config

    @property
    def host(self):
        """The current :class:`~woom.hosts.Host` instance (:class:`~woom.hosts.Host`)"""
        return self._host

    @property
    def name(self):
        """The task name (:class:`str`)"""
        return self.config.name

    @functools.cached_property
    def env(self):
        """Instance of :class:`woom.env.EnvConfig` specific to this task (:class:`~woom.env.EnvConfig`)"""

        # Get env from name, possibly empty
        env = self.host.get_env(self.config["content"]["env"]).copy()

        # Add woom variables
        env.vars_set.update(WOOM_TASK_NAME=self.name)
        run_dir = self.get_run_dir()
        if run_dir:
            env.vars_set.update(WOOM_RUN_DIR=run_dir)
        return env

    def get_run_dir(self):
        """Get the run directory"""
        run_dir = self.config["content"]["run_dir"]
        if run_dir is None:
            return ""
        if run_dir == "current":
            run_dir = os.getcwd()
        return run_dir.strip()

    def export_prolog(self):
        """Export the prolog of the batch script"""
        prolog = f'trap "false" {self.config["content"]["trap"]}'
        return prolog

    def export_env(self, params=None):
        """Export the environment declarations as bash lines"""
        return self.env.export(params)

    def export_run_dir(self):
        """Export the bash lines to move to the running directory"""
        run_dir = self.get_run_dir()
        if run_dir:
            return f"mkdir -p {run_dir} && cd {run_dir}"
        return ""

    def export_commandline(self):
        """Export the commandline as an bash lines"""
        return self.config["content"]["commandline"]

    def export_epilog(self):
        """Export the epilog of the batch script"""
        epilog = "echo $? > $WOOM_SUBMISSION_DIR/job.status\n"
        epilog += "exit $?\n"
        return epilog

    def render_content(self, params):
        """Export and render the task content with jinja, parameters and the :ref:`job.sh template <templates.job.sh>`

        Parameters
        ----------
        params: dict
            Parameters used for substitution

        Return
        ------
        str
        """
        params = params.copy()
        params["params"] = params
        return wrender.render(wrender.JINJA_ENV.get_template("job.sh"), params)

    def export_scheduler_options(self):
        """Export a dict of scheduler options

        Returns
        -------
        dict
        """
        if not self.host["scheduler"]:
            return {}
        opts = {
            "memory": self.config["submit"]["memory"],
            "time": self.config["submit"]["time"],
            "mail": self.config["submit"]["mail"],
            # "log_out": self.config["submit"]["log_out"],
            "extra": self.config["submit"]["extra"].dict(),
        }
        if self.config["submit"]["queue"]:
            opts["queue"] = self.host["queues"][self.config["submit"]["queue"]]
        return opts

    def export(self, params):
        return {
            "script_content": self.render_content(params),
            "scheduler_options": self.export_scheduler_options(),
        }
