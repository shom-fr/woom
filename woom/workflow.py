#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
The workflow core
"""
import os
import logging
import secrets
import functools
import shlex
import re
import json
import shutil

from . import WoomError
from . import conf as wconf
from . import util as wutil
from . import tasks as wtasks

CFGSPECS_FILE = os.path.join(os.path.dirname(__file__), "workflow.ini")

RE_SPLIT_COMMAS = re.compile(r"\s*,\s*").split


class WorkFlowError(Exception):
    pass


class Workflow:
    output_directories = ["log", "tasks"]

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
        self._task_tree = wtasks.TaskTree(self._config["stages"], self._config["groups"])
        self.logger.debug("Task tree:\n" + str(self._task_tree))
        self._dry = False

        # Paths
        self._workflow_dir = os.path.abspath(os.path.dirname(self._cfgfile))
        self._session["workflow_dir"] = self._workflow_dir
        os.environ["WOOM_WORKFLOW_DIR"] = self._workflow_dir
        self._paths = {
            "PATH": os.path.join(self._workflow_dir, "bin"),
            "PYTHONPATH": os.path.join(self._workflow_dir, "lib", "python"),
            "LIBRARY_PATH": os.path.join(self._workflow_dir, "lib"),
            "INCLUDE_PATH": os.path.join(self._workflow_dir, "include"),
        }
        self._app_path = []

        # Check app
        for key in "name", "conf", "exp":
            if self._config["app"][key]:
                if (
                    session["app_" + key]
                    and session["app_" + key].lower() != self._config["app"][key]
                ):
                    msg = "Workflow config and session app names are incompatible: '{}' != '{}'".format(
                        self._config["app"][key], session["app_" + key]
                    )
                    self.logger.error(msg)
                    raise WorkFlowError(msg)
                session["app_" + key] = self._config["app"][key]
                self._app_path.append(self._config["app"][key])

    def __str__(self):
        return f'<Workflow[cfgfile: "{self._cfgfile}", ' f'session: "{self.session.id}">\n'

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

    @functools.cached_property
    def task_tree(self):
        return self._task_tree.to_dict()

    @property
    def workflow_dir(self):
        """Where we are running the workflow"""
        return self._workflow_dir

    def get_app_path(self, sep=os.path.sep):
        """Typically `app/conf/exp` or ''"""
        return sep.join(self._app_path)

    def get_task_path(self, task_name, sep=os.path.sep):
        """Concatenate the :attr:`app_path` and the `task_name`"""
        return sep.join([task_name] + self._app_path)

    def get_submission_dir(self, task_name, task_cycle):
        """Get where batch script is created and submitted"""
        sdir = os.path.join(self.workflow_dir, "tasks", self.get_task_path(task_name))
        if task_cycle:
            sdir = os.path.join(sdir, task_cycle)
        return wutil.check_dir(sdir, dry=self._dry, logger=self.logger)

    def get_task_params(self, task_name, extra_params=None):
        """Get the params dictionary used to format a task command line

        Order with the last crushing the first:

        - [params] scalars
        - [app] scalars prepended with the "app_" prefix
        - [cycles] scalars prepended with the "cycles_" prefix
        - App path and task path
        - Host specific params included directories appended with the "dir" diffix
        - Task [[<task>]] scalars
        - Task@Host [[<task>]]/[[[<host>]]] scalars
        - Extra

        Parameters
        ----------
        task_name: str
            A valid task name
        extra_params: dict
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

        # App and task paths
        params["app_path"] = self.get_app_path()
        params["task_path"] = self.get_task_path(task_name)

        # Get host params
        params.update(self.host.get_params())

        # Get task specific params
        if task_name in self._config["params"]:
            params.update(wconf.strip_out_sections(self._config["params"][task_name]).dict())

            # Task specific params for this host
            if self.host.name in self._config["params"][task_name]:
                params.update(
                    wconf.strip_out_sections(
                        self._config["params"][task_name][self.host.name].dict()
                    )
                )

        # Extra parameters
        if extra_params:
            params.update(extra_params)

        return params

    def _get_submission_args_(self, task_name, cycle, depend, extra_params=None):
        # Get params
        params = self.get_task_params(task_name, extra_params=extra_params)

        # Store params to json
        if cycle:
            json_name = f"batch-{task_name}-{cycle.token}.json"
        else:
            json_name = f"batch-{task_name}.json"
        json_path = self.session.get_file_name("tasks", json_name)
        params["params_json"] = json_path

        # Submission script
        submission_dir = os.path.join(self.workflow_dir, "tasks", params["task_path"])
        if cycle:
            submission_dir = os.path.join(submission_dir, cycle.token)

        script_name = "batch.sh"
        script_path = os.path.join(submission_dir, script_name)
        wutil.check_dir(script_path, dry=self._dry, logger=self.logger)

        # Create task
        task_token = self.get_task_path(task_name, "-")
        if cycle:
            task_token = "-".join([task_token, cycle.token])
        params["task_token"] = task_token
        task = self.taskmanager.get_task(task_name, params, task_token)

        # Export paths in task environment variables
        task.env.prepend_paths(**self._paths)
        task.env.vars_set["WOOM_SUBMISSION_DIR"] = submission_dir
        task.env.vars_set["WOOM_APP_PATH"] = params["app_path"]
        task.env.vars_set["WOOM_TASK_PATH"] = params["task_path"]
        task.env.vars_set["WOOM_TASK_TOKEN"] = task_token
        if cycle:
            task.env.vars_set["WOOM_CYCLE_BEGIN_DATE"] = cycle["cycle_begin_date"]
            if cycle.is_interval:
                task.env.vars_set["WOOM_CYCLE_END_DATE"] = cycle["cycle_end_date"]
                task.env.vars_set["WOOM_CYCLE_DURATION"] = cycle["cycle_duration"]
                task.env.vars_set["WOOM_CYCLE_LABEL"] = cycle.label
                task.env.vars_set["WOOM_CYCLE_TOKEN"] = cycle.token

        # Get task bash code and submission options
        task_specs = task.export()

        # Submission options
        opts = task_specs["scheduler_options"].copy()
        opts["session"] = str(self.session)
        opts["name"] = task.name
        opts["token"] = task_token

        return {
            "batch_script": {"name": script_name, "content": task_specs["script_content"]},
            "params_json": {"name": json_name, "content": params},
            "submission": {"script": script_path, "opts": opts, "depend": depend},
        }

    def submit_task(self, task_name, cycle=None, depend=None, extra_params=None):
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
        submission_args = self._get_submission_args_(task_name, cycle, depend, extra_params)

        ## Store params as a json file in session cache (useful?)
        # json_name = submission_args["params_json"]["name"]
        # with self.session.open_file("batch_scripts", json_name, "w") as f:
        # json.dump(
        # submission_args["params_json"]["content"], f, indent=4, cls=wutil.WoomJSONEncoder
        # )
        ## json_path = f.name

        # Create the bash submission script
        batch_script = submission_args["submission"]["script"]
        self.logger.debug(f"Create bash script: {batch_script}")
        with open(batch_script, "w") as f:
            f.write(submission_args["batch_script"]["content"])
        self.logger.info(f"Created batch script: {batch_script}")

        # Submit it
        job = self.jobmanager.submit(**submission_args["submission"])

        return job

    def submit_task_fake(self, task_name, cycle=None, depend=None, extra_params=None):
        """Don't submit a task, just display it"""

        # Get the submission arguments
        submission_args = self._get_submission_args_(task_name, cycle, depend, extra_params)

        # Get submission command line
        jobargs = self.jobmanager.get_submission_args(**submission_args["submission"])
        cmdline = shlex.join(jobargs)

        jobid = str(secrets.randbelow(1000000))

        # Commandline
        content = "Fake submission:\n"
        content += " submission command ".center(80, "-") + "\n"
        content += cmdline + "\n"

        # Batch
        content += " batch script content ".center(80, "-") + "\n"
        content += submission_args["batch_script"]["content"] + "\n"

        # Json
        content += " params as json ".center(80, "-") + "\n"
        content += str(submission_args["params_json"]["content"]["params_json"]) + "\n"

        self.logger.debug(content)
        return jobid

    def run(self, dry=False):
        """Run the workflow by submiting all tasks"""
        self._dry = dry
        if dry:
            self.logger.debug("Running the workflow in fake mode")
        sequence_depend = []
        for stage in self.task_tree:
            self.logger.debug(f"Entering stage: {stage}")

            # Check that we have something to do
            if not self.task_tree[stage]:
                self.logger.debug("No sequence of task. Skipping...")
                continue

            # Get cycles for looping in time
            if stage == "cycles":
                try:
                    cycles = wutil.get_cycles(**self.config["cycles"])
                except Exception as err:
                    msg = "Error while computing dates of cycles:\n" + err.args[0]
                    self.logger.error(msg)
                    raise WoomError(msg)
                if cycles[0].is_interval:
                    self.logger.info(
                        "Cycling from {} to {} in {} time(s)".format(
                            cycles[0]["cycle_begin_date"],
                            cycles[-1]["cycle_end_date"],
                            len(cycles),
                        )
                    )
                else:
                    self.logger.info(
                        "Single cycle with unique date: {}".format(cycles[0]["cycle_begin_date"])
                    )

            else:
                cycles = [None]

            # Only the "cycles" stage is really looping
            for cycle in cycles:
                if stage == "cycles":
                    self.logger.debug("Running cycle: " + cycle.label)

                # Sequential loop on sequences aka substages
                for sequence, groups in self.task_tree[stage].items():
                    # Check that we have something to do
                    if not groups:
                        self.debug("No task to submit")
                        continue

                    self.logger.debug(f"Entering sequence: {sequence}")
                    new_sequence_depend = []

                    # Parallel loop on groups
                    for group in groups:
                        if len(group) > 1:
                            self.logger.debug("Group of {} sequential tasks:".format(len(group)))

                        # First task of group depend on last sequence
                        task_depend = sequence_depend

                        # Sequential sequential on group tasks
                        for task_name in group:
                            long_task = f"{stage}/{sequence}/{task_name}"
                            self.logger.debug(f"Submitting task: {long_task}")
                            self.logger.debug(
                                "  Dependencies: "
                                + ", ".join([str(job.jobid) for job in task_depend])
                            )
                            if dry:  # Fake mode
                                jobid = self.submit_task_fake(
                                    task_name, cycle=cycle, depend=task_depend
                                )

                            else:  # Real submission mode
                                jobid = self.submit_task(task_name, cycle=cycle, depend=task_depend)
                            self.logger.info(f"Submitted task: {long_task} with job id {jobid}")

                            # The next task of group depend on this job
                            task_depend = [jobid]

                        # The last job is added for next stage dependency
                        new_sequence_depend.append(jobid)

                    # Dependencies for the next sequence
                    sequence_depend = new_sequence_depend

                if stage == "cycles":
                    self.logger.info("Successfully submitted cycle: " + cycle.label)
                else:
                    self.logger.info("Successfully submitted stage: " + stage)
