#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Context for job script generation
"""
import json
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
        params = workflow.config["params"].dict()
        del params["hosts"], params["tasks"]
        for sec in "app", "cycles":
            for key, val in workflow.config[sec].items():
                self[f"{sec}_{key}"] = val
        self["app_path"] = workflow.get_app_path()
        self["app"] = workflow.config["app"].dict()
        self["app"]["path"] = workflow.get_app_path()

        # Host params
        self.update(workflow.host.get_params())
        if workflow.host.name in workflow.config["params"]["hosts"]:
            host_params = wconf.strip_out_sections(
                workflow.config["params"]["hosts"][workflow.host.name]
            ).dict()
            self.update(host_params)

        # Workflow directories
        self.update(workflow_dir=workflow.workflow_dir, log_dir=os.path.join(workflow.workflow_dir, "log"))

        # Current cycle
        self["cycle"] = cycle
        if isinstance(cycle, witers.Cycle):
            self.update(cycle.get_params())
            if isinstance(cycle.prev, witers.Cycle):
                self.update(cycle.prev.get_params(suffix="prev"))
            if isinstance(cycle.next, witers.Cycle):
                self.update(cycle.next.get_params(suffix="next"))

        # Current member
        self["member"] = member
        if member:
            self.update(member.params)

        # Current task
        self["task_name"] = task_name
        if task_name is None:
            self["task"] = None
        else:
            self["task"] = task = workflow.get_task(task_name)
            self.update(
                task_path=workflow.get_task_path(task_name, cycle, member),
                task_name=task_name,
            )

            # Task specific params
            if task_name in workflow.config["params"]["tasks"]:
                task_params = workflow.config["params"]["tasks"][task_name].dict()
                params.update(task_params)

                # if workflow.host.name in workflow.config["params"]["tasks"][task_name]:
                #     params.update(
                #         wconf.strip_out_sections(workflow.config["params"]["tasks"][task_name][workflow.host.name].dict())
                #     )

            # Paths
            task_submission_dir = workflow.get_task_submission_dir(task_name, cycle, member)
            self.update(
                task_run_dir=task.run_dir,
                task_submission_dir=task_submission_dir,
                task_script_path=os.path.join(task_submission_dir, "job.sh"),
            )
            for key in "run_dir", "submission_dir", "script_path":
                self[key] = self["task_" + key]  # backward compat
            self["run_dir"] = self["task_run_dir"]
            task.env.prepend_paths(**workflow.paths)

            # Environment
            self["task_env"] = self["env"] = task.env  # with backward compat

            # Json file
            self["task_context_json"] = self["context_json"] = os.path.join(
                task_submission_dir, "context.json"
            )  # with backward compat

        # Extra params
        if extra_params:
            params.update(extra_params)

        # Store params and set env vars
        self["params"] = params
        # Convert self.data (the underlying dict) to env vars, not self (which would cause recursion)
        env_vars = wutil.dict_to_env_vars(
            self.data, exclude=["context", "env_vars", "os", "logger", "config"]
        )
        self["context"] = self
        self["env_vars"] = env_vars
        if self.task:
            self.task.env.vars_set.update(self["env_vars"])

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
    def params(self):
        """A :class:`dict` of user parameters as declared in the 'params' section of the workflow configuration"""
        return self["params"]

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

    def __enter__(self):
        self._old_workflow_context = self.workflow._context
        self._old_task_context = self.task._context
        self.workflow.context = self
        self.task.context = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self, '_old_workflow_context'):
            self.workflow._context = self._old_workflow_context
        if hasattr(self.workflow, 'context'):
            delattr(self.workflow, 'context')
        if hasattr(self, '_old_task_context'):
            self.task._context = self._old_task_context
        if self.task.has_context():
            delattr(self.task, 'context')

    def to_json(self):
        """Export context to json"""
        if "context_json" in self:
            content = self.copy()
            if "context" in content:
                del content["context"]
            with open(self["context_json"], "w") as f:
                json.dump(content, f, indent=4, cls=wutil.WoomJSONEncoder)
