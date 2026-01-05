#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task manager
"""

import functools
import logging
import os
import re

import configobj

from . import conf as wconf
from . import render as wrender
from . import util as wutil
from .__init__ import WoomError

thisdir = os.path.dirname(__file__)

#: Specifications file for tasks configuration
CFGSPECS_FILE = os.path.join(thisdir, "tasks.ini")

RE_SPLIT_COMMAS = re.compile(r"\s*,\s*").split

#: Default tasks configuration
CFG_DEFAULT_FILE = os.path.join(thisdir, "tasks.cfg")


#: Functions that generate artifact paths taking a context as first argument
ARTIFACTS_GENERATORS = {}


class TaskError(WoomError):
    pass


class TaskTree:
    """Postprocess configuration to build a task tree"""

    def __init__(self, stages, groups=None):
        """Initialize a task tree

        Parameters
        ----------
        stages : configobj.Section
            Workflow stages configuration
        groups : configobj.Section, optional
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

    def get_task_stage(self, task_name):
        """Get the stage name of task

        It can be either "prolog", "cycles", "epilog" or None
        """
        dd = self.to_dict()
        for stage, scontent in dd.items():
            if not scontent:
                continue
            for substage, sscontent in scontent.items():
                for gt in sscontent:
                    if task_name in gt:
                        return stage


class TaskManager:
    """Manager of :class:`Task` instances"""

    def __init__(self, host):
        """Initialize a task manager

        Parameters
        ----------
        host : Host
            Host instance
        """
        self._configs = []
        self._config = wconf.load_cfg(CFG_DEFAULT_FILE, CFGSPECS_FILE, interpolation=False, list_values=True)
        self._host = host
        self.logger = logging.getLogger(__name__)
        self._config_files = []

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
        cfg = wconf.load_cfg(cfgfile, CFGSPECS_FILE, interpolation=False, list_values=True)
        self._config_files.append(cfgfile)
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

            # Convert old artifacts specifications
            for task, content in self._config.items():
                for artifact_name in content["artifacts"].scalars:
                    path = content["artifacts"][artifact_name]
                    del content["artifacts"][artifact_name]
                    content["artifacts"][artifact_name] = {"path": path, "check": True, "callable": False}

    def to_json_entry(self):
        return self._config_files

    @classmethod
    def from_config_files(cls, host, *config_files):
        taskmanager = cls(host)
        for config_file in config_files:
            taskmanager.load(config_file)
        return taskmanager

    @property
    def config(self):
        """The tasks configuration as loaded from the :file:`tasks.cfg` (:class:`~configobj.ConfigObj`)"""
        return self._config

    @property
    def host(self):
        """The associated :class:`~woom.hosts.Host` instance"""
        return self._host

    @functools.lru_cache
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
        """Initialize a task

        Parameters
        ----------
        taskconfig : configobj.Section
            Task configuration section
        host : Host
            Host instance
        """
        self._config = taskconfig
        self._host = host
        self._context = None
        self.logger = logging.getLogger(__name__)

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

    @property
    def is_blocking(self):
        """It is blocking?"""
        return self.config["submit"]["blocking"]

    def set_context(self, context):
        """Set the context to be used for jinja rendering

        Parameters
        ----------
        context: woom.context.Context
        """
        if context is not None:
            context["task"] = self
        self._context = context

    @property
    def context(self):
        """Context to be used for rendering"""
        if self._context is None:
            raise TaskError("The context must be set before using it")
        self._context["task"] = self
        return self._context

    @context.setter
    def context(self, context):
        self.set_context(context)

    @context.deleter
    def context(self):
        self._context = None

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
        """The artifacts specifications as a dict (:class:`dict`)"""
        return self.get_artifacts()

    def get_artifacts(self):
        """Get the artifacts raw paths as a dict

        It generates the list of paths of all artifacts.

        Parameters
        ----------
        context: dict, woom.context.Context
            A dict that contain the context used to render the job script.
            It is required when the artifact paths must be generated by a function.

        Returns
        -------
        dict
            Keys are artifacts names and values are lists of raw artifact paths
        """
        artifacts = {}
        for name, specs in self.config["artifacts"].items():
            artifacts[name] = self.get_artifact_path(name)
        return artifacts

    @functools.lru_cache
    def get_artifact_path(self, name):
        """Get the path of a single artifact from its name

        Parameters
        ----------
        name: str
            Artifact name

        Returns
        -------
        str or list(str)
            A single pr a lists of raw artifact paths
        """
        specs = self.config["artifacts"][name].dict()
        # if not isinstance(specs["path"], list):
        #     specs["paths"] = [specs["paths"]]
        if specs["callable"]:
            func_name = specs["path"]
            if isinstance(func_name):
                func_name = func_name[0]
            if func_name not in ARTIFACTS_GENERATORS:
                raise TaskError(f"Artifact generator function not found: {func_name}")
            kwargs = dict(self.context)
            kwargs.update(specs["kwargs"])
            path = ARTIFACTS_GENERATORS[func_name](**kwargs)
            # if isinstance(paths, str):
            #     paths = [paths]
        else:
            path = specs["path"]
        return path

    @functools.cached_property
    def run_dir(self):
        """Run directory"""
        run_dir = self.config["content"]["run_dir"]
        if run_dir is None:
            return ""
        if run_dir == "current":
            run_dir = os.getcwd()
        return run_dir.strip()
        # return self.get_run_dir()

    @property
    def commandline(self):
        """Commandline as an bash lines"""
        return self.config["content"]["commandline"]

    def render_artifacts(self):
        """Check that artifact paths are absolute and render them as dict

        Parameters
        ----------
        context: dict, woom.context.Context
            Parameters used for substitution

        Return
        ------
        dict
            Keys are artifacts names and values are lists of rendered artifact paths
        """
        if not self.artifacts:
            return {}
        artifacts = {}
        for name, path in self.get_artifacts().items():
            artifacts[name] = []
            paths = [path] if isinstance(path, str) else path
            for path_ in paths:
                rendered = wrender.render(path_.strip(), self.context)
                if not os.path.isabs(path_):
                    if self.run_dir:
                        rendered = os.path.join(self.run_dir, rendered)
                    else:
                        raise TaskError(
                            f"Rendered artifact '{name}' of task '{self.name}' is not absolute "
                            "and task run_dir is not defined. Please fix it!"
                        )
                artifacts[name].append(rendered)
        return artifacts

    def render_content(self):
        """Render the task content with jinja

        Rendering uses parameters and the template defined in the configuration,
        which defaults to :ref:`job.sh<templates.job.sh>`.

        Return
        ------
        str
        """
        # context = context.copy()
        # context["context"] = context
        template = wrender.JINJA_ENV.get_template(self.config["content"]["template"])
        return wrender.render(template, self.context)

    def fill_templates(self, dry=False):
        """Fill static user template files"""
        for name in self.config["fill"].sections:
            config = self.config["fill"][name]

            # Render paths
            template_file = wrender.render(config["template"], self.context)
            destination = wrender.render(config["destination"], self.context)

            # Fill template
            self.logger.debug(f"Fill template '{name}': {template_file} → {destination}")
            template = wrender.JINJA_ENV.get_template(template_file)
            content = wrender.render(template, self.context)

            # Write destination
            destination = wutil.check_dir(destination, dry=dry, logger=self.logger)
            if not dry:
                with open(destination, "w") as f:
                    f.write(content)
            self.logger.info(f"Filled ttemplate '{name}': {template_file} → {destination}")

    def export_scheduler_options(self):
        """Export a dict of scheduler options

        Returns
        -------
        dict
        """
        if not self.host.config["scheduler"]:
            return {}
        opts = wconf.strip_out_sections(self.config["submit"]).dict()
        if self.config["submit"]["queue"]:
            opts["queue"] = self.host.config["queues"][self.config["submit"]["queue"]]
        return opts

    def export(self):
        """Export the task specification with rendering

        .. warning:: The :attr:`context` must be set with :meth:`set_context`
        """
        return {
            "script_content": self.render_content(),
            "scheduler_options": self.export_scheduler_options(),
            "artifacts": self.render_artifacts(),
        }
