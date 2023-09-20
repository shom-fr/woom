#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
The workflow core
"""
import os
import logging
import secrets
import datetime
import functools
import shlex

import configobj

from . import conf as wconf

CFGSPECS_FILE = os.path.join(os.path.dirname(__file__), "workflow.ini")


class WorkFlowError(Exception):
    pass


class Workflow:
    def __init__(self, cfgfile, session, taskmanager):
        self.logger = logging.getLogger(__name__)
        if isinstance(cfgfile, str):
            self._cfgfile = cfgfile
            self._config = wconf.load_cfg(cfgfile, CFGSPECS_FILE)
        else:
            self._config = cfgfile
            self._cfgfile = self._config.filename
        self._tm = taskmanager
        self._session = session
        for key in "name", "conf", "exp":
            if self._config["app"][key]:
                if (
                    session["app_" + key]
                    and session["app_" + key].lower()
                    != self._config["app"][key]
                ):
                    msg = "Workflow config and session app names are incompatible: '{}' != '{}'".format(
                        self._config["app"][key], session["app_" + key]
                    )
                    self.logger.error(msg)
                    raise WorkFlowError(msg)
                session["app_" + key] = self._config["app"][key]

    def __str__(self):
        return (
            f'<Workflow[cfgfile: "{self._cfgfile}", '
            f'session: "{self.session.id}">\n'
        )

    @property
    def config(self):
        return self._config

    @property
    def taskmanager(self):
        return self._tm

    @property
    def host(self):
        return self.taskmanager.host

    @property
    def session(self):
        return self._session

    @functools.cached_property
    def jobmanager(self):
        """The :mod:`~woom.job` manager instance"""
        return self.host.get_jobmanager(self.session)

    def get_task_params(self, task_name, extra_params=None):
        """Get the params dictionnary used to format a task command line

        Order with the last crushing the first:

        - [params] scalars
        - [app] scalars prepended with the "app_" prefix
        - Host specific params included directories appended with the "dir" diffix
        - Task [[<task>]] scalars
        - Task@Host [[<task>]]/[[[<host>]]] scalars
        - Extra

        Parameters
        ----------
        task_name: str
            A valid task name
        extra: dict
            Extra parameters to include in params

        Return
        ------
        dict
        """
        # Base with interpolation
        params = configobj.ConfigObj(
            list_values=False, interpolation="template"
        )

        # Workflow generic params
        params.merge(wconf.strip_out_sections(self._config["params"]))

        # App
        for key, val in self.config["app"].items():
            if val:
                params["app_" + key] = val

        # Get host params
        params.merge(self.host.get_params())

        # Get task specific params
        if task_name in self._config["params"]:
            params.merge(
                wconf.strip_out_sections(self._config["params"][task_name])
            )

            # Task specific params for this host
            if self.host.name in self._config["params"][task_name]:
                params.merge(
                    wconf.strip_out_sections(
                        self._config["params"][task_name][self.host.name]
                    )
                )

        # Extra parameters
        if extra_params:
            params.merge(extra_params)

        # Subsitutions
        params = configobj.ConfigObj(
            params, list_values=False, interpolation=True
        )

        return params.dict()

    def _get_submission_args_(self, task_name, extra_params, depend):
        # Get params
        params = self.get_task_params(task_name, extra_params=extra_params)

        # Get task bash code and submission options
        task_specs = self.taskmanager.export_task(task_name, params)

        # Create the bash submission script in cache
        token = secrets.token_hex(8)
        date = datetime.datetime.utcnow()
        script_name = f"batch-{task_name}-{date:%Y-%m-%d-%H-%M-%S}-{token}.sh"

        # Submission optipns
        script_path = self.session.get_file_name("batch_scripts", script_name)
        opts = task_specs["scheduler_options"].copy()
        opts["session"] = str(self.session)

        return {
            "batch_script": {
                "name": script_name,
                "content": task_specs["script_content"],
            },
            "submission": {
                "script": script_path,
                "opts": opts,
                "depend": depend,
            },
        }

    def submit_task(self, task_name, extra_params=None, depend=None):
        """Submit a task

        Parameters
        ----------
        task_name: str
            A valid task name

        Return
        ------
        str
            Job id
        """
        # Get the submission arguments
        submission_args = self._get_submission_args_(
            task_name, extra_params=extra_params, depend=depend
        )

        # Create the bash submission script in cache
        script_name = submission_args["batch_script"]["name"]
        with self.session.open_file("batch_scripts", script_name, "w") as f:
            f.write(submission_args["batch_script"]["content"])

        # Submit it
        job = self.jobmanager.submit(**submission_args["submission"])

        return job

    def export_task_to_dict(self, task_name, extra_params=None, depend=None):
        # Get the submission arguments
        submission_args = self._get_submission_args_(
            task_name, extra_params, depend
        )

        # Get submission command line
        jobargs = self.jobmanager.get_submission_args(
            **submission_args["submission"]
        )
        cmdline = shlex.join(jobargs)

        return {
            "submission_commandline": cmdline,
            "batch_script_content": submission_args["batch_script"]["content"],
        }

    def run(self, startdate=None, enddate=None, freq=None, ncycle=None):
        """Run the workflow by submiting all tasks"""
        if self.config["stages"]["dry_run"]:
            self.logger.debug("Running the workflow in fake mode")
        depend = []
        cycle_params = {}
        for stage in self.config["stages"].sections:
            self.logger.debug(f"Entering stage: {stage}")
            if not self.config["stages"][stage].scalars:
                self.logger.debug("No sequence of task. Skipping...")
                continue
            if stage == "cycles":
                extra_params = cycle_params
                self.logger.debug(f"Cycle: {cycle_params}")
            else:
                extra_params = None
            for sequence, task_names in self.config["stages"][stage].items():
                if not task_names:
                    self.debug("No task to submit")
                    continue
                self.logger.debug(f"Entering sequence: {sequence}")
                new_depend = []
                for task_name in task_names:
                    long_task = f"{stage}/{sequence}/{task_name}"
                    self.logger.debug(f"Submitting task: {long_task}")
                    if self.config["stages"]["dry_run"]:
                        res = self.export_task_to_dict(
                            task_name, extra_params=extra_params, depend=depend
                        )
                        jobid = str(secrets.randbelow(1000000))
                        content = "Fake submission:\n"
                        content += (
                            " submission command ".center(80, "-") + "\n"
                        )
                        content += res["submission_commandline"] + "\n"
                        content += (
                            " batch script content ".center(80, "-") + "\n"
                        )
                        content += res["batch_script_content"] + "\n"
                        self.logger.debug(content)
                    else:
                        jobid = self.submit_task(
                            task_name, extra_params=extra_params, depend=depend
                        )
                    self.logger.info(
                        f"Submitted task: {long_task} with job id {jobid}"
                    )
                    new_depend.append(jobid)
                depend = new_depend
