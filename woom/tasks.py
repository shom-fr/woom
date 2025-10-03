#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task manager
"""

import functools
import os
import re

import configobj

from . import conf as wconf
from . import render as wrender
from .__init__ import WoomError

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
        cfg = wconf.load_cfg(cfgfile, CFGSPECS_FILE, list_values=False)
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
        if self.run_dir:
            env.vars_set.update(WOOM_RUN_DIR=self.run_dir)
        return env

    @property
    def artifacts(self):
        """The artifacts as a dict of file names (:class:`dict`)"""
        return self.config["artifacts"]

    def get_run_dir(self):
        """Get the run directory"""
        run_dir = self.config["content"]["run_dir"]
        if run_dir is None:
            return ""
        if run_dir == "current":
            run_dir = os.getcwd()
        return run_dir.strip()

    @functools.cached_property
    def run_dir(self):
        return self.get_run_dir()

    def export_env(self, params=None):
        """Export the environment declarations as bash lines"""
        return self.env.render(params)

    def export_run_dir(self):
        """Export the bash lines to move to the running directory"""
        if self.run_dir:
            return f"mkdir -p {self.run_dir} && cd {self.run_dir}"
        return ""

    def export_commandline(self):
        """Export the commandline as an bash lines"""
        return self.config["content"]["commandline"]

    def export_artifacts_checking(self):
        """Export commandlines to check the existence of artifacts"""
        if not self.artifacts:
            return ""
        checks = ""
        for name, path in self.artifacts.items():
            checks += 'test -f "' + path + '" || { echo artifact ' + name + '="' + path + '"; exit 1; }\n'
        return checks

    def render_artifacts(self, params):
        """Check that artifact paths are absolute and render them as dict"""
        if not self.artifacts:
            return {}
        artifacts = {}
        for name, path in self.config["artifacts"].items():
            rendered = wrender.render(path.strip(), params)
            if not os.path.isabs(path):
                if self.run_dir:
                    rendered = os.path.join(self.run_dir, rendered)
                else:
                    raise TaskError(
                        f"Rendered artifact '{name}' of task '{self.name}' is not absolute "
                        "and task run_dir is not defined. Please fix it!"
                    )
            artifacts[name] = rendered
        return artifacts

    def render_content(self, params):
        """Render the task content with jinja

        Rendering uses parameters and the :ref:`job.sh template <templates.job.sh>`

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
            "artifacts": self.render_artifacts(params),
        }
