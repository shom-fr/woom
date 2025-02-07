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

import pandas as pd

from . import WoomError
from . import conf as wconf
from . import util as wutil
from . import tasks as wtasks
from . import job as wjob

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
            self._config = wconf.load_cfg(cfgfile, CFGSPECS_FILE, list_values=True)
        else:
            self._config = cfgfile
            self._cfgfile = self._config.filename
        stages = {}
        for stage in "prolog", "cycles", "epilog":  # re-order
            stages[stage] = self._config["stages"][stage]
        self._tm = taskmanager
        # self._session = session = taskmanager.session
        # self._config["params"]["session_id"] = session.id
        self._task_tree = wtasks.TaskTree(stages, self._config["groups"])
        self.logger.debug("Task tree:\n" + str(self._task_tree))
        self._dry = False
        self._upate = False

        # Cylces
        if self.task_tree["cycles"]:
            self._cycles = wutil.get_cycles(**self.config["cycles"])

        # Paths
        self._workflow_dir = os.path.abspath(os.path.dirname(self._cfgfile))
        # self._session["workflow_dir"] = self._workflow_dir
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
                # if (
                # session["app_" + key]
                # and session["app_" + key].lower() != self._config["app"][key]
                # ):
                # msg = "Workflow config and session app names are incompatible: '{}' != '{}'".format(
                # self._config["app"][key], session["app_" + key]
                # )
                # self.logger.error(msg)
                # raise WorkFlowError(msg)
                # session["app_" + key] = self._config["app"][key]
                self._app_path.append(self._config["app"][key])

    def __str__(self):
        return f'<Workflow[cfgfile: "{self._cfgfile}">\n'
        # return f'<Workflow[cfgfile: "{self._cfgfile}", ' f'session: "{self.session.id}">\n'

    @property
    def config(self):
        return self._config

    @property
    def taskmanager(self):
        return self._tm

    @property
    def host(self):
        return self.taskmanager.host

    # @property
    # def session(self):
    # return self._session

    @functools.cached_property
    def jobmanager(self):
        """The :mod:`~woom.job` manager instance"""
        return self.host.get_jobmanager()  # self.session)

    @functools.cached_property
    def task_tree(self):
        return self._task_tree.to_dict()

    @property
    def cycles(self):
        return self._cycles

    @property
    def workflow_dir(self):
        """Where we are running the workflow"""
        return self._workflow_dir

    def get_app_path(self, sep=os.path.sep):
        """Typically `app/conf/exp` or ''"""
        return sep.join(self._app_path)

    def get_task_path(self, task_name, cycle=None, sep=os.path.sep):
        """Concatenate the :attr:`app_path`, the cycle and the `task_name`"""
        parts = self._app_path.copy()
        if cycle:
            parts.append(str(cycle))
        parts.append(task_name)
        return sep.join(parts)

    def get_submission_dir(self, task_name, cycle=None, create=True):
        """Get where batch script is created and submitted"""
        sdir = os.path.join(self.workflow_dir, "jobs", self.get_task_path(task_name, cycle))
        if not create:
            return sdir
        return wutil.check_dir(sdir, dry=self._dry, logger=self.logger)

    def get_task_params(self, task_name, cycle=None, extra_params=None):
        """Get the params dictionary used to format a task command line

        Order with the last crushing the first:

        - ``[params]`` scalars
        - ``[app]`` scalars prepended with the `"app_"` prefix
        - ``[cycles]`` scalars prepended with the `"cycles_"` prefix
        - App path and task path
        - Host specific params included directories appended with the `"dir"` diffix
        - Extra

        Parameters
        ----------
        task_name: str
            A valid task name
        cycle: woom.util.Cycle, str, None
            Current cycle or None
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
        env_vars = dict((key.upper(), value) for key, value in params.items())

        # Subsections
        for sec in "app", "cycles":
            for key, val in self.config[sec].items():
                params[f"{sec}_{key}"] = val

        # App and task paths
        params["app_path"] = self.get_app_path()
        params["task_path"] = self.get_task_path(task_name, cycle)

        # Get host params
        params.update(self.host.get_params())

        # Task specific params
        if task_name in self._config["params"]["tasks"]:
            task_params = wconf.strip_out_sections(
                self._config["params"]["tasks"][task_name]
            ).dict()
            env_vars.update((key.upper(), value) for key, value in task_params.items())
            params.update(task_params)

            # if self.host.name in self._config["params"]["tasks"][task_name]:
            #     params.update(
            #         wconf.strip_out_sections(self._config["params"]["tasks"][task_name][self.host.name].dict())
            #     )

        # Host specific params
        if self.host.name in self._config["params"]["hosts"]:
            host_params = wconf.strip_out_sections(
                self._config["params"]["hosts"][self.host.name]
            ).dict()
            env_vars.update((key.upper(), value) for key, value in host_params.items())
            params.update(host_params)

            # Task specific params for this host
        # Extra parameters
        if extra_params:
            params.update(extra_params)

        # User params as environment variables
        params.update(env_vars=env_vars)

        return params

    def _get_submission_args_(
        self, task_name, cycle, depend, extra_params=None, cycle_prev=None, cycle_next=None
    ):
        # Get params
        params = self.get_task_params(task_name, cycle=cycle, extra_params=extra_params)
        env_vars = params.pop("env_vars")

        # Update with cycles
        if isinstance(cycle, wutil.Cycle):
            params.update(cycle.get_params())
        if isinstance(cycle_prev, wutil.Cycle):
            params.update(cycle.get_params(suffix="prev"))
        if isinstance(cycle_next, wutil.Cycle):
            params.update(cycle.get_params(suffix="next"))

        ## Store params to json
        # if cycle:
        # json_name = f"batch-{task_name}-{cycle.token}.json"
        # else:
        # json_name = f"batch-{task_name}.json"
        # json_path = self.session.get_file_name("tasks", json_name)
        # params["params_json"] = json_path

        # Submission script
        submission_dir = self.get_submission_dir(task_name, cycle)
        script_name = "job.sh"
        script_path = os.path.join(submission_dir, script_name)
        # json_path = os.path.join(submission_dir, "params.json")
        # params["params_json"] = json_path
        wutil.check_dir(script_path, dry=self._dry, logger=self.logger)

        # Create task
        task = self.taskmanager.get_task(task_name)

        # Export paths in task environment variables
        task.env.prepend_paths(**self._paths)
        task.env.vars_set["WOOM_WORKFLOW_DIR"] = self._workflow_dir
        task.env.vars_set["WOOM_SUBMISSION_DIR"] = submission_dir
        task.env.vars_set["WOOM_LOG_DIR"] = os.path.join(submission_dir, "log")
        task.env.vars_set["WOOM_TASK_NAME"] = task_name
        task.env.vars_set["WOOM_TASK_PATH"] = params["task_path"]
        task.env.vars_set["WOOM_APP_PATH"] = params["app_path"]
        task.env.vars_set.update(env_vars)
        for key in "app_name", "app_conf", "app_exp":
            if params[key] is not None:
                task.env.vars_set["WOOM_" + key.upper()] = params[key]
        if isinstance(cycle, wutil.Cycle):
            task.env.vars_set.update(cycle.get_env_vars())
            if cycle_prev:
                task.env.vars_set.update(cycle_prev.get_env_vars("prev"))
            if cycle_next:
                task.env.vars_set.update(cycle_next.get_env_vars("next"))

        # Get task bash code and submission options
        task_specs = task.export(params)

        # Submission options
        opts = task_specs["scheduler_options"].copy()
        opts["name"] = task.name

        return {
            "script": script_path,
            "content": task_specs["script_content"],
            "opts": opts,
            "depend": depend,
        }
        # return {
        # "batch_script": {"name": script_name, "content": task_specs["script_content"]},
        ##"params_json": {"name": json_name, "content": params},
        # "submission": {"script": script_path, "opts": opts, "depend": depend},
        # }

    def submit_task(
        self,
        task_name,
        cycle=None,
        depend=None,
        extra_params=None,
        cycle_prev=None,
        cycle_next=None,
    ):
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
            task_name, cycle, depend, extra_params, cycle_prev, cycle_next
        )

        ## Store params as a json file in session cache (useful?)
        # json_name = submission_args["params_json"]["name"]
        # with self.session.open_file("batch_scripts", json_name, "w") as f:
        # json.dump(
        # submission_args["params_json"]["content"], f, indent=4, cls=wutil.WoomJSONEncoder
        # )
        ## json_path = f.name

        # Create the bash submission script
        batch_script = submission_args["script"]
        self.logger.debug(f"Create bash script: {batch_script}")
        with open(batch_script, "w") as f:
            f.write(submission_args["content"])
        self.logger.info(f"Created batch script: {batch_script}")
        del submission_args["content"]  # no longer needed since on disk

        # Submit it
        job = self.jobmanager.submit(**submission_args)

        return job

    def submit_task_fake(
        self,
        task_name,
        cycle=None,
        depend=None,
        extra_params=None,
        cycle_prev=None,
        cycle_next=None,
    ):
        """Don't submit a task, just display it"""

        # Get the submission arguments
        submission_args = self._get_submission_args_(
            task_name, cycle, depend, extra_params, cycle_prev, cycle_next
        )
        batch_content = submission_args.pop("content")

        # Get submission command line
        jobargs = self.jobmanager.get_submission_args(**submission_args)
        cmdline = shlex.join(jobargs)

        jobid = str(secrets.randbelow(1000000))

        # Commandline
        content = "Fake submission:\n"
        content += " submission command ".center(80, "-") + "\n"
        content += cmdline + "\n"

        # Batch
        content += " batch script content ".center(80, "-") + "\n"
        content += batch_content + "\n"

        ## Json
        # content += " params as json ".center(80, "-") + "\n"
        # content += str(submission_args["params_json"]["content"]["params_json"]) + "\n"

        self.logger.debug(content)
        return jobid

    def get_task_status(self, task_name, cycle=None):
        """Get the job status of a task

        Return
        ------
        woom.job.JobStatus
            Job status
        """
        submission_dir = self.get_submission_dir(task_name, cycle, create=False)

        # Not submitted
        if not os.path.exists(submission_dir):
            return wjob.JobStatus["NOTSUBMITTED"]

        # Job info
        json_file = os.path.join(submission_dir, "job.json")
        if os.path.exists(json_file):
            job = self.jobmanager.load_job(json_file, append=True)
        else:
            return wjob.JobStatus["NOTSUBMITTED"]

        # Finish with success
        status_file = os.path.join(submission_dir, "job.status")
        if os.path.exists(status_file):
            with open(status_file) as f:
                status = int(f.read())
            if status:
                status = wjob.JobStatus["ERROR"]
            else:
                status = wjob.JobStatus["SUCCESS"]
            status.jobid = job.jobid
            return status

        # Running or killed
        return job.get_status()

    def clean_task(self, task_name, cycle=None):
        """Remove job specific files for this task

        The following files are removed:

        - :file:`job.sh`
        - :file:`job.err`
        - :file:`job.out`
        - :file:`job.json`
        - :file:`job.status`
        """
        # self.logger.debug(f"Cleaning task: {task_name}")
        submission_dir = self.get_submission_dir(task_name, cycle)
        for ext in ("sh", "err", "out", "json", "status"):
            fname = os.path.join(submission_dir, "job." + ext)
            if os.path.exists(fname):
                if not self._dry:
                    os.remove(fname)
                self.logger.debug(f"Removed: {fname}")

    def run(self, dry=False, update=False):
        """Run the workflow by submiting all tasks"""
        self._dry = dry
        self._update = update
        if dry:
            self.logger.debug("Running the workflow in fake mode")
        if update:
            self.logger.debug("Running the workflow in update mode")
        sequence_depend = []
        for stage in self.task_tree:
            self.logger.debug(f"Entering stage: {stage}")

            # Check that we have something to do
            if not self.task_tree[stage]:
                self.logger.debug("No sequence of task. Skipping...")
                continue

            # Get cycles for looping in time
            if stage == "cycles":
                cycles = self._cycles
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
                cycles = [stage]

            # Only the "cycles" stage is really looping
            for i, cycle in enumerate(cycles):
                if stage == "cycles":
                    self.logger.debug("Running cycle: " + cycle.label)
                    if i == 0:
                        cycle_prev = None
                    else:
                        cycle_prev = cycles[i - 1]
                    if i == len(cycles) - 1:
                        cycle_next = None
                    else:
                        cycle_next = cycles[i + 1]
                else:
                    cycle_prev = cycle_next = None

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
                        job = None
                        for task_name in group:
                            long_task = f"{stage}/{sequence}/{task_name}"
                            self.logger.debug(f"Dealing with task: {long_task}")

                            # Check status
                            status = self.get_task_status(task_name, cycle)
                            if status.is_running():
                                raise WorkFlowError(
                                    "Can't run a task that is already running. Aborting... Run 'woom kill {status.jobid}' to kill the associated job before re-running."
                                )

                            if update:
                                if status.name is wjob.JobStatus.SUCCESS:
                                    self.logger.debug(f"Skip update of task: {long_task}")
                                    continue

                                elif status is wjob.JobStatus.ERROR:
                                    self.logger.warning(
                                        "Existing job task led to error. Re-running..."
                                    )
                                elif status is wjob.JobStatus.UNKNOWN:
                                    self.logger.warning(
                                        "Unknown status for existing task job task. Re-running..."
                                    )

                            # Clean
                            self.logger.debug(f"Cleaning task: {long_task}")
                            self.clean_task(task_name, cycle)

                            # Submit
                            self.logger.debug(f"Submitting task: {long_task}")
                            self.logger.debug(
                                "  Dependencies: " + ", ".join([str(job) for job in task_depend])
                            )
                            kwtask = dict(
                                task_name=task_name,
                                cycle=cycle,
                                depend=task_depend,
                                cycle_prev=cycle_prev,
                                cycle_next=cycle_next,
                            )
                            if dry:  # Fake mode
                                job = self.submit_task_fake(**kwtask)

                            else:  # Real submission mode
                                job = self.submit_task(**kwtask)
                                if job is None:
                                    raise WorkFlowError(
                                        "Task submission aborted: {long_task}. Stopping workflow..."
                                    )
                            self.logger.info(f"Submitted task: {long_task} with job id {job}")

                            # The next task of group depend on this job
                            task_depend = [job]

                        # The last job is added for next stage dependency
                        if job:
                            new_sequence_depend.append(job)

                    # Dependencies for the next sequence
                    sequence_depend = new_sequence_depend

                if stage == "cycles":
                    self.logger.info("Successfully submitted cycle: " + cycle.label)
                else:
                    self.logger.info("Successfully submitted stage: " + stage)

    def show_overview(self):
        """Display an overview of the workflow, like its task tree and cycles"""
        if self._app_path:
            print("{:#^80}".format(" APP "))
            for key in ["name", "conf", "exp"]:
                value = self._config["app"][key]
                if value:
                    print(f"{key}: {value}")
        print("{:#^80}".format(" TASK TREE "))
        print(str(self._task_tree))
        print("{:#^80}".format(" CYCLES "))
        for cycle in self._cycles:
            print(cycle.label)

    def iter_tasks(self):
        """Generator of iterating over the tasks and cycles

        Yield
        -----
        task_name, cycle
        """
        for stage in self.task_tree:
            if not self.task_tree[stage]:
                continue
            cycles = self.cycles if stage == "cycles" else [stage]
            for cycle in cycles:
                for sequence, groups in self.task_tree[stage].items():
                    if not groups:
                        continue
                    for group in groups:
                        for task_name in group:
                            yield task_name, cycle

    __iter__ = iter_tasks

    @property
    def submission_dirs(self):
        """Generator of submission directories computed from the task tree"""
        for task_name, cycle in self.iter_tasks():
            yield self.get_submission_dir(task_name, cycle, create=False)

    def get_status(self, running=False):
        """Get the workflow task status as a :class:`pandas.DataFrame`

        Parameters
        ----------
        running: bool
            Select only running jobs

        Return
        ------
        pandas.DataFrame
        """
        data = []
        # index = []
        columns = ["STATUS", "JOBID", "TASK", "CYCLE", "SUBMISSION DIR"]
        for task_name, cycle in self:
            status = self.get_task_status(task_name, cycle)
            if running and not status.is_running():
                continue
            submdir = self.get_submission_dir(task_name, cycle)[len(self._workflow_dir) + 1 :]
            row = [status.name, status.jobid, task_name, cycle, submdir]
            data.append(row)
        return pd.DataFrame(data, columns=columns)

    def show_status(self, running=False, tablefmt="rounded_outline"):
        """Show the status of all the tasks of the wokflow

        Parameters
        ----------
        running: bool
            Show only running jobs
        """
        print(
            self.get_status(
                running=running,
            ).to_markdown(index=False, tablefmt=tablefmt)
        )

    def kill(self, jobid=None, task_name=None, cycle=None):
        """Kill all running jobs specific to this workflow

        Parameters
        ----------
        jobid: str, list(str), None
            KIll only this jobid if it belongs to the workflow
        task_name: str, None:
            Select this task
        cycle: str, woom.util.Cycle, None
            Select this cycle
        """
        if not jobid:
            jobids = []
        elif isinstance(jobid, str):
            jobids = [jobid]
        else:
            jobids = jobid
        for task_name_, cycle_ in self.iter_tasks():
            if task_name and task_name_ != task_name:
                continue
            if cycle and str(cycle) != str(cycle_):
                continue
            submdir = self.get_submission_dir(task_name_, cycle_, create=False)
            task_path = self.get_task_path(task_name_, cycle_)
            json_file = os.path.join(submdir, "job.json")
            if os.path.exists(json_file):
                job = self.jobmanager.load_job(json_file, append=True)
                if jobids and job.jobid not in jobids:
                    continue
                if job.is_running():
                    self.logger.debug(f"Killing jobid: {job.jobid} ({task_path})")
                    job.kill()
                    # job.set_status("KILLED")
                    msg = f"Killed jobid: {job.jobid} ({task_path})"
                    self.logger.debug(msg)
                    print(msg)

        else:
            self.logger.info("No job to kill")
