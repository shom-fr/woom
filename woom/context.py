#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Context for job script generation
"""
import os
from collections import UserDict

from . import conf as wconf
from . import iters as witers
from . import util as wutil


class Context(UserDict):
    """Dict-like context that is used by jinja to fill templates

    Its content is accessible with keys like::

        context = Context(workflow, task_name, cycle, member)
        print(context["task"].name)

    See :ref:`inputs_context` for a list of available keys.

    .. note:: Some of the items are available as attributes, like :attr:`workflow`.

    """

    def __init__(self, workflow, task_name=None, cycle=None, member=None, extra_params=None):
        """Initialize rendering context

        Parameters
        ----------
        workflow : Workflow
            Workflow instance
        task_name : str, optional
            Task name
        cycle : Cycle, str, optional
            Current cycle
        member : Member, optional
            Ensemble member
        extra_params : dict, optional
            Extra parameters for rendering
        """
        initialdata = {
            "workflow": workflow,
            "host": workflow.host,
            "taskmanager": workflow.taskmanager,
            "jobmanager": workflow.jobmanager,
            "logger": workflow.logger,
            "task_tree": workflow.task_tree,
            "config": workflow.config,
            "cycles": workflow.cycles,
            "nmembers": workflow.nmembers,
            "members": workflow.members,
            "paths": workflow.paths,
            "app_path": workflow.get_app_path(),
            "env_vars": workflow.config["env_vars"].dict(),
            "os": os,
        }
        super().__init__(initialdata)

        # Config subsections
        params = wconf.strip_out_sections(workflow.config["params"]).dict()
        for sec in "app", "cycles":
            for key, val in workflow.config[sec].items():
                params[f"{sec}_{key}"] = val
        params["app_path"] = workflow.get_app_path()

        # Host params
        params.update(workflow.host.get_params())
        if workflow.host.name in workflow.config["params"]["hosts"]:
            host_params = wconf.strip_out_sections(
                workflow.config["params"]["hosts"][workflow.host.name]
            ).dict()
            params.update(host_params)

        # Workflow directories
        params.update(workflow_dir=workflow.workflow_dir, log_dir=os.path.join(workflow.workflow_dir, "log"))

        # Current cycle
        params["cycle"] = cycle
        if isinstance(cycle, witers.Cycle):
            params.update(cycle.get_params())
            if isinstance(cycle.prev, witers.Cycle):
                params.update(cycle.prev.get_params(suffix="prev"))
            if isinstance(cycle.next, witers.Cycle):
                params.update(cycle.next.get_params(suffix="next"))

        # Current member
        params["member"] = member
        if member:
            params.update(member.params)

        # Current task
        params["task_name"] = task_name
        if task_name is None:
            self["task"] = None
        else:
            self["task"] = task = workflow.get_task(task_name)
            params.update(
                task_path=workflow.get_task_path(task_name, cycle, member),
                task_name=task_name,
            )

            # Task specific params
            if task_name in workflow.config["params"]["tasks"]:
                task_params = wconf.strip_out_sections(workflow.config["params"]["tasks"][task_name]).dict()
                params.update(task_params)  # too dangerous?

                # if workflow.host.name in workflow.config["params"]["tasks"][task_name]:
                #     params.update(
                #         wconf.strip_out_sections(workflow.config["params"]["tasks"][task_name][workflow.host.name].dict())
                #     )

            # Paths
            submission_dir = workflow.get_task_submission_dir(task_name, cycle, member)
            params.update(
                run_dir=task.run_dir,
                submission_dir=submission_dir,
                script_path=os.path.join(submission_dir, "job.sh"),
            )
            task.env.prepend_paths(**workflow.paths)

            # Environment
            self["env"] = task.env

        # Extra params
        if extra_params:
            params.update(extra_params)

        # Store params and set env vars
        self.set_params(params)
        self["params"] = params
        self["context"] = self

    def __repr__(self):
        return (
            "<Context(<Worflow>, task_name={self['task_name']},"
            " cycle={self['cycle']}, member={self['member']})>"
        )

    def copy(self):
        return Context(self.workflow)

    @property
    def workflow(self):
        """The current :class:`~woom.workflow.Workflow` instance"""
        return self["workflow"]

    @property
    def config(self):
        """The workflow configuration"""
        return self["config"]

    @property
    def env_vars(self):
        """A :class:`dict` of environment variables as declared in the workflow configuration"""
        return self["env_vars"]

    @property
    def task(self):
        """The current :class:`~woom.tasks.Tasks` instance or `None`"""
        return self.get("task")

    @property
    def cycle(self):
        """The current :class:`~woom.iters.Cycle` instance or `None`"""
        return self.get("cycle")

    @property
    def member(self):
        """The current :class:`~woom.iters.Member` instance or `None`"""
        return self.get("member")

    def set_params(self, params):
        """Fill the dict and declare environment variables prefixed with WOOM\_"""
        self.update(params)
        self["env_vars"].update(wutil.params2env_vars(params))
        if self.task:
            self.task.env.vars_set.update(self["env_vars"])

    def __enter__(self):
        self.workflow.context = self
        self.task.context = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del self.workflow.context
        del self.task.context
