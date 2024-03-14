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
import re

from . import WoomError
from . import conf as wconf
from . import util as wutil

CFGSPECS_FILE = os.path.join(os.path.dirname(__file__), "workflow.ini")

RE_SPLIT_COMMAS = re.compile(r"\s*,\s*").split


class WorkFlowError(Exception):
    pass


class Workflow:
    def __init__(self, cfgfile, taskmanager):
        self.logger = logging.getLogger(__name__)
        if isinstance(cfgfile, str):
            self._cfgfile = cfgfile
            self._config = wconf.load_cfg(cfgfile, CFGSPECS_FILE)
        else:
            self._config = cfgfile
            self._cfgfile = self._config.filename
        self._tm = taskmanager
        self._session = session = taskmanager.session
        self._config["params"]["session_id"] = session.id

        # Checkp app
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
        - [cycles] scalars prepended with the "cycles_" prefix
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

        See also
        --------
        woom.util.subst_dict
        """

        # Workflow generic params
        params = wconf.strip_out_sections(self._config["params"]).dict()

        # Subsections
        for sec in "app", "cycles":
            for key, val in self.config[sec].items():
                params[f"{sec}_{key}"] = val

        # Get host params
        params.update(self.host.get_params())

        # Get task specific params
        if task_name in self._config["params"]:
            params.update(
                wconf.strip_out_sections(
                    self._config["params"][task_name]
                ).dict()
            )

            # Task specific params for this host
            if self.host.name in self._config["params"][task_name]:
                params.update(
                    wconf.strip_out_sections(
                        self._config["params"][task_name][
                            self.host.name
                        ].dict()
                    )
                )

        # Extra parameters
        if extra_params:
            params.update(extra_params)

        return params

    def _get_submission_args_(self, task_name, extra_params, depend):
        # Get params
        params = self.get_task_params(task_name, extra_params=extra_params)

        # Token
        token = secrets.token_hex(8)
        date = datetime.datetime.utcnow()

        # Store params to json
        json_name = f"batch-{task_name}-{date:%Y-%m-%d-%H-%M-%S}-{token}.json"
        json_path = self.session.get_file_name("batch_scripts", json_name)
        params["params_json"] = json_path

        # Get task bash code and submission options
        task_specs = self.taskmanager.export_task(task_name, params)

        # Create the bash submission script in cache
        script_name = f"batch-{task_name}-{date:%Y-%m-%d-%H-%M-%S}-{token}.sh"

        # Submission options
        script_path = self.session.get_file_name("batch_scripts", script_name)
        opts = task_specs["scheduler_options"].copy()
        opts["session"] = str(self.session)

        return {
            "batch_script": {
                "name": script_name,
                "content": task_specs["script_content"],
            },
            "params_json": {"name": json_name, "content": params},
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

        # Store params as a json file in cache
        json_name = submission_args["params_json"]["name"]
        with self.session.open_file("batch_scripts", json_name, "w") as f:
            f.write(submission_args["params_json"]["content"])

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
            "params_json": submission_args["params_json"],
        }

    def run(self, dry=False):
        """Run the workflow by submiting all tasks"""
        if dry:
            self.logger.debug("Running the workflow in fake mode")
        sequence_depend = []
        for stage in self.config["stages"].sections:
            self.logger.debug(f"Entering stage: {stage}")

            # Check that we have something to do
            if not self.config["stages"][stage].scalars:
                self.logger.debug("No sequence of task. Skipping...")
                continue

            # Get cycles for looping in time
            if stage == "cycles":
                try:
                    cycles = wutil.get_cycles(**self.config["cycles"])
                except Exception as err:
                    msg = (
                        "Error while computing dates of cycles:\n"
                        + err.args[0]
                    )
                    self.logger.error(msg)
                    raise WoomError(msg)
                if "cycle_end_date" not in cycles[0]:
                    self.logger.info(
                        "Single cycle with unique date: {}".format(
                            cycles[0]["cycle_begin_date"]
                        )
                    )
                else:
                    self.logger.info(
                        "Cycling from {} to {} in {} time(s)".format(
                            cycles[0]["cycle_begin_date"],
                            cycles[-1]["cycle_end_date"],
                            len(cycles),
                        )
                    )

            else:
                cycles = [None]

            # Only the "cycles" stage is really looping
            for cycle_params in cycles:
                if stage == "cycles":
                    if "cycle_end_date" not in cycle_params:
                        self.logger.debug(
                            "Running cycle: {}".format(
                                cycle_params["cycle_begin_date"]
                            )
                        )
                    else:
                        self.logger.debug(
                            "Running cycle: {} -> {}".format(
                                cycle_params["cycle_begin_date"],
                                cycle_params["cycle_end_date"],
                            )
                        )
                for sequence, task_names in self.config["stages"][
                    stage
                ].items():
                    # List of tasks
                    task_names = RE_SPLIT_COMMAS(task_names)

                    # Check that we have something to do
                    if not task_names:
                        self.debug("No task to submit")
                        continue

                    self.logger.debug(f"Entering sequence: {sequence}")
                    new_sequence_depend = []

                    # Loop on task or group of task names
                    for task_group_name in task_names:
                        if task_group_name not in self.config["groups"]:
                            group = [task_group_name]
                        else:
                            group = RE_SPLIT_COMMAS(
                                self.config["groups"][task_group_name]
                            )
                            long_task = f"{stage}/{sequence}/{task_group_name}"
                            self.logger.debug(
                                f"Group of ordered tasks: {long_task}"
                            )

                        # First task of group depend on last sequence
                        task_depend = sequence_depend

                        for task_name in group:
                            long_task = f"{stage}/{sequence}/{task_name}"
                            self.logger.debug(f"Submitting task: {long_task}")
                            if dry:  # Fake mode
                                res = self.export_task_to_dict(
                                    task_name,
                                    extra_params=cycle_params,
                                    depend=task_depend,
                                )
                                jobid = str(secrets.randbelow(1000000))

                                # Commandline
                                content = "Fake submission:\n"
                                content += (
                                    " submission command ".center(80, "-")
                                    + "\n"
                                )
                                content += res["submission_commandline"] + "\n"

                                # Batch
                                content += (
                                    " batch script content ".center(80, "-")
                                    + "\n"
                                )
                                content += res["batch_script_content"] + "\n"

                                # Json
                                content += (
                                    " params as json ".center(80, "-") + "\n"
                                )
                                content += (
                                    str(
                                        res["params_json"]["content"][
                                            "params_json"
                                        ]
                                    )
                                    + "\n"
                                )

                                self.logger.debug(content)

                            else:  # Real submission mode
                                jobid = self.submit_task(
                                    task_name,
                                    extra_params=cycle_params,
                                    depend=task_depend,
                                )
                            self.logger.info(
                                f"Submitted task: {long_task} with job id {jobid}"
                            )

                            # The next task of group depend on this job
                            task_depend = jobid

                        # The last job is added for next stage dependency
                        new_sequence_depend.append(jobid)

                    # Dependencies for the next sequence
                    sequence_depend = new_sequence_depend
