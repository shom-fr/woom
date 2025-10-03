#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
The workflow core
"""
import functools
import glob
import logging
import os
import re
import secrets
import shlex
import shutil

import pandas as pd

from . import WoomError
from . import conf as wconf
from . import iters as witers
from . import job as wjob
from . import render as wrender
from . import tasks as wtasks
from . import util as wutil

CFGSPECS_FILE = os.path.join(os.path.dirname(__file__), "workflow.ini")

RE_SPLIT_COMMAS = re.compile(r"\s*,\s*").split

STATUS2COLOR = {
    "(FAILED|ERROR|KILLED)": "bold_red",
    "(EXITING|COMPLETING|UNKNOWN)": "bold_yellow",
    "SUCCESS": "bold_green",
    "(PENDING|INQUEUE)": "bold",
}


class WorkFlowError(WoomError):
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
        self._task_tree = wtasks.TaskTree(stages, self._config["groups"])
        self.logger.debug("Task tree:\n" + str(self._task_tree))
        self._dry = False
        self._upate = False

        # Workflow dir
        self._workflow_dir = os.path.abspath(os.path.dirname(self._cfgfile))
        os.environ["WOOM_WORKFLOW_DIR"] = self._workflow_dir

        # Setup extensible templates BEFORE any rendering
        self.user_template_dir = wrender.setup_template_loader(self._workflow_dir)
        if os.path.exists(self.user_template_dir):
            self.logger.info(f"User templates directory enabled: {self.user_template_dir}")

        # Cycles
        if self.task_tree["cycles"]:
            cycles_conf = self.config["cycles"].dict()
            self._cycles_indep = cycles_conf.pop("indep")
            self._cycles = witers.gen_cycles(**cycles_conf)
        else:
            self._cycles = []

        # Ensemble
        self._members = witers.gen_ensemble(
            self.config["ensemble"]["size"],
            skip=self.config["ensemble"]["skip"],
            **self.config["ensemble"]["iters"],
        )
        self._nmembers = len(self._members)

        # Other paths
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
                self._app_path.append(self._config["app"][key])

    def __str__(self):
        return f'<Workflow[cfgfile: "{self._cfgfile}">\n'

    @property
    def config(self):
        """Workflow :class:`~configobj.ConfigObj` configuration instance"""
        return self._config

    def __getitem__(self, key):
        return self.config[key]

    @property
    def taskmanager(self):
        """Current :class:`~woom.tasks.TaskManager` instance"""
        return self._tm

    @property
    def host(self):
        """Current :class:`~woom.hosts.Host` instance"""
        return self.taskmanager.host

    @functools.cached_property
    def jobmanager(self):
        """Current :mod:`~woom.job` manager instance"""
        return self.host.get_jobmanager()  # self.session)

    @functools.cached_property
    def task_tree(self):
        return self._task_tree.to_dict()

    @property
    def cycles(self):
        """List of :class:`~woom.iters.Cycle`"""
        return self._cycles

    @property
    def nmembers(self):
        """Number of ensemble members"""
        return self._nmembers

    @property
    def members(self):
        """List of :class:`~woom.iters.Member`"""
        return self._members

    @property
    def workflow_dir(self):
        """Where we are running the workflow"""
        return self._workflow_dir

    def get_app_path(self, sep=os.path.sep):
        """Typically `app/conf/exp` or ''"""
        return sep.join(self._app_path)

    def get_task_path(self, task_name, cycle=None, member=None, sep=os.path.sep):
        """Concatenate the :attr:`app_path`, the cycle and, `task_name` and the member label"""
        parts = self._app_path.copy()
        if cycle:
            parts.append(str(cycle))
        parts.append(task_name)
        if member is not None and self.get_task_members(task_name):
            parts.append(member.label)
        return sep.join(parts)

    def get_submission_dir(self, task_name, cycle=None, member=None, create=True):
        """Where the batch script is created and submitted"""
        sdir = os.path.join(self.workflow_dir, "jobs", self.get_task_path(task_name, cycle, member))
        if not create:
            return sdir
        return wutil.check_dir(sdir, dry=self._dry, logger=self.logger)

    @functools.lru_cache
    def get_task_inputs(self, task_name, cycle=None, member=None, extra_params=None):
        """Get the params dictionary used to format a task command line and environment variables

        Order with the last crushing the first:

        - ``[params]`` scalars
        - ``[app]`` scalars prepended with the `"app_"` prefix
        - ``[cycles]`` scalars prepended with the `"cycles_"` prefix
        - Ensemble member
        - App path and task path
        - Host specific params included directories appended with the `"dir"` sufffix
        - Extra

        Parameters
        ----------
        task_name: str
            A valid task name
        cycle: woom.util.Cycle, str, None
            Current cycle or None
        member: None, woom.iters.Member
            Member number of the ensemble, starting from 1
        extra_params: dict
            Extra parameters to include in params

        Return
        ------
        dict
            Parameters for substitutions
        dict
            Environement variables
        """

        # Workflow generic params
        params = wconf.strip_out_sections(self._config["params"]).dict()
        env_vars = dict(("WOOM_" + key.upper(), value) for key, value in params.items())

        # Workflow environment variables
        env_vars.update(self._config["env_vars"])

        # Subsections
        for sec in "app", "cycles":
            for key, val in self.config[sec].items():
                params[f"{sec}_{key}"] = val
                env_vars.update(wutil.params2env_vars({f"{sec}_{key}": val}))

        # App and task paths
        params["app_path"] = self.get_app_path()
        params["task_path"] = self.get_task_path(task_name, cycle, member)
        params["task_name"] = task_name
        env_vars.update(wutil.params2env_vars(params, select=["app_path", "task_path", "task_name"]))

        # Get host params
        params.update(self.host.get_params())

        # Current cycle
        params["cycle"] = cycle
        if isinstance(cycle, witers.Cycle):
            params.update(cycle.get_params())
            env_vars.update(cycle.get_env_vars())
            if isinstance(cycle.prev, witers.Cycle):
                params.update(cycle.prev.get_params(suffix="prev"))
                env_vars.update(cycle.prev.get_env_vars(suffix="prev"))
            if isinstance(cycle.next, witers.Cycle):
                params.update(cycle.next.get_params(suffix="next"))
                env_vars.update(cycle.next.get_env_vars(suffix="next"))

        # Current member
        params["member"] = member
        if member:
            params.update(member.params)
            env_vars.update(member.env_vars)
        else:
            params["nmembers"] = self.nmembers
            env_vars["WOOM_NMEMBERS"] = str(self.nmembers)

        # Task specific params
        if task_name in self._config["params"]["tasks"]:
            task_params = wconf.strip_out_sections(self._config["params"]["tasks"][task_name]).dict()
            env_vars.update(("WOOM_" + key.upper(), value) for key, value in task_params.items())
            # params.update(task_params) # too dangerous!

            # if self.host.name in self._config["params"]["tasks"][task_name]:
            #     params.update(
            #         wconf.strip_out_sections(self._config["params"]["tasks"][task_name][self.host.name].dict())
            #     )

        # Host specific params
        if self.host.name in self._config["params"]["hosts"]:
            host_params = wconf.strip_out_sections(self._config["params"]["hosts"][self.host.name]).dict()
            env_vars.update(("WOOM_" + key.upper(), value) for key, value in host_params.items())
            params.update(host_params)

            # Task specific params for this host

        # Other parameters
        if extra_params:
            params.update(extra_params)
        task = self.get_task(task_name)
        submission_dir = self.get_submission_dir(task_name, cycle, member)
        params.update(
            workflow=self,
            logger=self.logger,
            workflow_dir=self._workflow_dir,
            task=task,
            run_dir=task.get_run_dir(),
            submission_dir=self.get_submission_dir(task_name, cycle, member),
            log_dir=os.path.join(self._workflow_dir, "log"),
            script_path=os.path.join(submission_dir, "job.sh"),
        )
        env_vars.update(
            wutil.params2env_vars(
                params,
                select=["workflow_dir", "run_dir", "submission_dir", "log_dir", "script_path"],
            )
        )
        return params, env_vars

    def get_task_members(self, task_name):
        """Get the list of members if applicable or None"""
        if not self.nmembers:
            return
        if self.config["ensemble"]["tasks"] and task_name in self.config["ensemble"]["tasks"]:
            return self.members

    @functools.lru_cache
    def get_task(self, task_name):
        """Shortcut to ``self.taskmanager.get_task(task_name)``"""
        return self.taskmanager.get_task(task_name)

    def get_run_dir(self, task_name, cycle=None, member=None):
        """Get where the command lines are executed in the script"""
        params, _ = self.get_task_inputs(task_name, cycle, member)
        task = self.get_task(task_name)
        return wrender.render(task.get_run_dir(), params)

    @functools.lru_cache
    def get_task_artifacts(self, task_name, cycle=None, member=None):
        """Get rendered artifacts for a given task"""
        params, _ = self.get_task_inputs(task_name, cycle, member)
        task = self.get_task(task_name)
        return task.render_artifacts(params)

    def get_artifact(self, artifact_name, task_name, cycle=None, member=None):
        """Get the path of an artifact for a given task"""
        return self.get_task_artifacts(task_name, cycle, member)[artifact_name]

    def _get_submission_args_(self, task_name, cycle, member, depend, extra_params=None):
        # Create task
        task = self.get_task(task_name)

        # Get params
        params, env_vars = self.get_task_inputs(
            task_name, cycle=cycle, member=member, extra_params=extra_params
        )
        params["task"] = task

        # Submission script
        script_path = params["script_path"]
        wutil.check_dir(script_path, dry=self._dry, logger=self.logger)

        # Fill task environment variables
        task.env.prepend_paths(**self._paths)
        task.env.vars_set.update(env_vars)

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
            'artifacts': task_specs["artifacts"],
        }

    def submit_task(self, task_name, cycle=None, member=None, depend=None, extra_params=None):
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
        submission_args = self._get_submission_args_(task_name, cycle, member, depend, extra_params)

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
        member=None,
        depend=None,
        extra_params=None,
    ):
        """Don't submit a task, just display it"""

        # Get the submission arguments
        submission_args = self._get_submission_args_(task_name, cycle, member, depend, extra_params)
        batch_content = submission_args.pop("content")
        artifacts = submission_args.pop("artifacts")

        # Get submission command line
        jobargs = self.jobmanager.get_submission_command(**submission_args)
        cmdline = shlex.join(jobargs)

        jobid = str(secrets.randbelow(1000000))

        # Commandline
        content = "Fake submission:\n"
        content += " submission command ".center(50, "-") + "\n"
        content += cmdline + "\n"

        # Batch
        content += " batch script content ".center(50, "-") + "\n"
        content += batch_content + "\n"

        # Artifacts
        if artifacts:
            content += " artifacts ".center(50, "-") + "\n"
            for name, path in artifacts.items():
                content += f"{name}: {path}\n"

        content += "-" * 50

        self.logger.debug(content)
        return jobid

    def get_task_status(self, task_name, cycle=None, member=None):
        """Get the job status of a task

        Return
        ------
        woom.job.JobStatus
            Job status
        """
        submission_dir = self.get_submission_dir(task_name, cycle, member, create=False)

        # Not submitted
        if not os.path.exists(submission_dir):
            return wjob.JobStatus["NOTSUBMITTED"]

        # Job info
        json_file = os.path.join(submission_dir, "job.json")
        if os.path.exists(json_file):
            job = self.jobmanager.load_job(json_file, append=True)
        else:
            return wjob.JobStatus["NOTSUBMITTED"]

        # Walltime exceeded
        out_file = os.path.join(submission_dir, "job.out")
        if os.path.exists(out_file):
            with open(out_file) as f:
                content = f.read()
            if "PBS: job killed: walltime" in content and "Terminated" in content:
                status = wjob.JobStatus["FAILED"]
                status.jobid = job.jobid
                return status

        # Walltime exceeded
        # FIXME: to be integrated in job
        out_file = os.path.join(submission_dir, "job.out")
        if os.path.exists(out_file):
            with open(out_file) as f:
                content = f.read()
            if "PBS: job killed: walltime" in content and "Terminated" in content:
                status = wjob.JobStatus["FAILED"]
                status.jobid = job.jobid
                return status

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

    def clean_task(self, task_name, cycle=None, member=None):
        """Remove job specific files for this task

        The following files are removed:

        - :file:`job.sh`
        - :file:`job.err`
        - :file:`job.out`
        - :file:`job.json`
        - :file:`job.status`
        """
        # self.logger.debug(f"Cleaning task: {task_name}")
        submission_dir = self.get_submission_dir(task_name, cycle, member)
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
        stage_depend = []
        for stage in self.task_tree:
            self.logger.debug(f"Entering stage: {stage}")

            # Check that we have something to do
            if not self.task_tree[stage]:
                self.logger.debug("No sequence of task. Skipping...")
                continue

            # Get cycles for looping in time
            if stage == "cycles":
                cycles = self._cycles
                if len(self._cycles) > 1:
                    indep = "independant " if self._cycles_indep else ""
                    if cycles[0].is_interval:
                        self.logger.info(
                            "Cycling on {}intervals from {} to {} in {} time(s)".format(
                                indep, cycles[0].begin_date, cycles[-1].end_date, len(cycles)
                            )
                        )
                    else:
                        self.logger.info(
                            "Cycling on {}dates from {} to {} in {} time(s)".format(
                                indep, cycles[0].date, cycles[-1].date, len(cycles)
                            )
                        )
                else:
                    self.logger.info("Single cycle with unique date: {}".format(cycles[0].date))

            else:
                cycles = [stage]

            # Only the "cycles" stage is really looping
            stage_jobs = []
            sequence_depend = stage_depend
            for i, cycle in enumerate(cycles):
                if stage == "cycles":
                    self.logger.debug("Running cycle: " + cycle.label)
                    if self._cycles_indep:  # independant cycles depend always on the last stage
                        sequence_depend = stage_depend

                # Sequential loop on sequences aka substages
                for sequence, groups in self.task_tree[stage].items():
                    # Check that we have something to do
                    if not groups:
                        self.debug("No task to submit")
                        continue

                    self.logger.debug(f"Entering sequence: {sequence}")

                    # Parallel loop on groups
                    sequence_jobs = []
                    for group in groups:
                        if len(group) > 1:
                            self.logger.debug("Group of {} sequential tasks:".format(len(group)))

                        # First task of groups depend on last sequence
                        task_depend = sequence_depend

                        # Sequential sequential on group tasks
                        job = None
                        for task_name in group:
                            # Parallel on ensemble members
                            task_jobs = []
                            for member in self.get_task_members(task_name) or [None]:
                                long_task = f"{stage}/{sequence}/{task_name}"
                                if member:
                                    long_task += f"/{member.label}"
                                self.logger.debug(f"Running task: {long_task}")

                                # Check status
                                status = self.get_task_status(task_name, cycle, member)
                                if status.is_running():
                                    raise WorkFlowError(
                                        "Can't run a task that is already running. Aborting... "
                                        "Run 'woom kill {status.jobid}' to kill the associated "
                                        "job before re-running."
                                    )

                                if update:
                                    if status.name is wjob.JobStatus.SUCCESS:
                                        self.logger.debug(f"Skip update of task: {long_task}")
                                        continue

                                    elif status is wjob.JobStatus.ERROR:
                                        self.logger.warning("Existing job task led to error. Re-running...")
                                    elif status is wjob.JobStatus.UNKNOWN:
                                        self.logger.warning(
                                            "Unknown status for existing task job task. Re-running..."
                                        )

                                # Clean
                                self.logger.debug(f"Cleaning task: {long_task}")
                                self.clean_task(task_name, cycle, member)

                                # Submit
                                self.logger.debug(f"Submitting task: {long_task}")
                                jobids = ", ".join([str(job) for job in task_depend])
                                self.logger.debug(f"  Dependencies: {jobids}")
                                kwtask = dict(
                                    task_name=task_name,
                                    cycle=cycle,
                                    member=member,
                                    depend=task_depend,
                                )
                                if dry:  # Fake mode
                                    job = self.submit_task_fake(**kwtask)

                                else:  # Real submission mode
                                    job = self.submit_task(**kwtask)
                                    if job is None:
                                        raise WorkFlowError(
                                            f"Task submission aborted: {long_task}. Stopping workflow..."
                                        )
                                depending = f" depending on [{jobids}]" if task_depend else ""
                                self.logger.info(f"Submitted task: {long_task} with job id {job}{depending}")

                                # The next task of this group depend on this job member
                                task_jobs.append(job)

                            # Dependencies for the next task in the group
                            task_depend = task_jobs

                        # The last jobs of this group are added to sequence jobs
                        sequence_jobs.extend(task_jobs)

                    # Dependencies for the next sequence
                    sequence_depend = sequence_jobs

                # Stage jobs
                if stage == "cycles" and self._cycles_indep:  # parallel independant cycles
                    stage_jobs.extend(sequence_jobs)
                else:
                    stage_jobs = sequence_jobs

                if stage == "cycles":
                    self.logger.info("Successfully submitted cycle: " + cycle.label)
                else:
                    self.logger.info("Successfully submitted stage: " + stage)

            stage_depend = stage_jobs

    def show_overview(self):
        """Display an overview of the workflow, like its task tree and cycles"""
        # App
        if self._app_path:
            print("{:#^80}".format(" APP "))
            for key in ["name", "conf", "exp"]:
                value = self._config["app"][key]
                if value:
                    print(f"{key}: {value}")

        # Task tree
        print("{:#^80}".format(" TASK TREE "))
        print(str(self._task_tree))

        # Cycles
        print("{:#^80}".format(" CYCLES "))
        if self.task_tree["cycles"]:
            for cycle in self._cycles:
                print(cycle.label)
        else:
            print("No cycle")

        # Ensemble
        print("{:#^80}".format(" ENSEMBLE "))
        if self.nmembers:
            print(f"size: {self.nmembers}")
            print("tasks: " + ", ".join(self.config["ensemble"]["tasks"]))
            for name, values in self._config["ensemble"]["iters"].items():
                print(f"{name}: " + ", ".join([str(v) for v in values]))
        else:
            print("no member")

    def __iter__(self):
        """Generator for iterating over the tasks, cycles and members

        Yield
        -----
        task_name, cycle, member
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
                            for member in self.get_task_members(task_name) or [None]:
                                yield task_name, cycle, member

    @property
    def submission_dirs(self):
        """Generator of submission directories computed from the task tree"""
        for task_name, cycle, member in self:
            yield self.get_submission_dir(task_name, cycle, member, create=False)

    def get_status(self, running=False, colorize=True):
        """Get the workflow task status as a :class:`pandas.DataFrame`

        Parameters
        ----------
        running: bool
            Select only running jobs
        colorize: bool
            Colorize the status

        Return
        ------
        pandas.DataFrame
        """
        data = []
        # index = []
        for task_name, cycle, member in self:
            status = self.get_task_status(task_name, cycle, member)
            if running and not status.is_running():
                continue
            submdir = self.get_submission_dir(task_name, cycle, member)[len(self._workflow_dir) + 1 :]
            colored_status = wutil.colorize(status.name, STATUS2COLOR, colorize=colorize)
            row = [colored_status, status.jobid, task_name, cycle, submdir]
            if self.nmembers:
                if member is None:
                    row.insert(-1, "")
                else:
                    row.insert(-1, f"{member}/{self.nmembers}")
            data.append(row)
        columns = ["STATUS", "JOBID", "TASK", "CYCLE", "SUBMISSION DIR"]
        if self.nmembers:
            columns.insert(-1, "MEMBER")
        return pd.DataFrame(data, columns=columns)

    def show_status(self, running=False, tablefmt="rounded_outline", colorize=True):
        """Show the status of all the tasks of the wokflow

        Parameters
        ----------
        running: bool
            Show only running jobs
        tablefmt: str
            Table format (see tabulate package)
        colorize: bool
            Colorize the status
        """
        print(
            self.get_status(
                running=running,
                colorize=colorize,
            ).to_markdown(index=False, tablefmt=tablefmt)
        )

    def get_artifacts(self, task_name=None, cycle=None, member=None):
        """Get artifacts as a :class:`pandas.DataFrame`

        Parameters
        ----------
        jobid: str, list(str), None
            KIll only this jobid if it belongs to the workflow
        task_name: str, None:
            Select this task
        cycle: str, woom.util.Cycle, None
            Select this cycle
        member: int, None
            Ensemble member id
        """
        data = []
        for task_name_, cycle_, member_ in self:
            if task_name and task_name_ != task_name:
                continue
            if cycle and str(cycle) != str(cycle_):
                continue
            if member is not None and str(member) != str(member_):
                continue
            for i, (name, path) in enumerate(self.get_task_artifacts(task_name_, cycle_, member_).items()):
                tn = task_name_ if not i else ""
                data.append([tn, name, path, os.path.exists(path)])

        columns = ["TASK", "ARTIFACT", "PATH", "EXISTS?"]
        return pd.DataFrame(data, columns=columns)

    def show_artifacts(self, tablefmt="rounded_outline"):
        """Show the status of all the tasks of the wokflow"""
        print(self.get_artifacts().to_markdown(index=False, tablefmt=tablefmt))

    def kill(self, jobid=None, task_name=None, cycle=None, member=None):
        """Kill all running jobs specific to this workflow

        Parameters
        ----------
        jobid: str, list(str), None
            KIll only this jobid if it belongs to the workflow
        task_name: str, None:
            Select this task
        cycle: str, woom.util.Cycle, None
            Select this cycle
        member: int, None
            Ensemble member id
        """
        if not jobid:
            jobids = []
        elif isinstance(jobid, str):
            jobids = [jobid]
        else:
            jobids = jobid
        for task_name_, cycle_, member_ in self:
            if task_name and task_name_ != task_name:
                continue
            if cycle and str(cycle) != str(cycle_):
                continue
            if member is not None and str(member) != str(member_):
                continue
            submdir = self.get_submission_dir(task_name_, cycle_, member_, create=False)
            task_path = self.get_task_path(task_name_, cycle_, member_)
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

    def get_run_dirs(self):
        """Get the run directories as :class:`pandas.DataFrame`

        Return
        ------
        pandas.DataFrame
        """
        data = []
        # index = []
        for task_name, cycle, member in self:
            run_dir = self.get_run_dir(task_name, cycle, member)
            row = [task_name, cycle, run_dir]
            if self.nmembers:
                if member is None:
                    row.insert(-1, "")
                else:
                    row.insert(-1, f"{member}/{self.nmembers}")
            data.append(row)
        columns = ["TASK", "CYCLE", "RUN DIR"]
        if self.nmembers:
            columns.insert(-1, "MEMBER")
        return pd.DataFrame(data, columns=columns)

    def show_run_dirs(self, tablefmt="rounded_outline"):
        """Show the status of all the tasks of the wokflow"""
        print(self.get_run_dirs().to_markdown(index=False, tablefmt=tablefmt))

    def clean(
        self,
        submission_dirs=True,
        log_files=True,
        run_dirs=False,
        artifacts=False,
        extra_files=None,
        dry=False,
    ):
        """Remove working files and directories

        Parameters
        ----------
        subssion_dirs: bool
            Remove the submission directories. They are sub-directories of the workflow directory.
        log_files: bool
            Remove the main log file and its backups.
        run_dirs: bool
            Remove the run directory. Since the may be overriden by
            the user, be cautious!
        artifacts: bool
            Remove files declared as artifacts.
        extra_files: None, list
            A list of file or glob patterns to remove.
        """
        # Loop on tasks
        self.logger.debug("Starting to clean...")
        nitems = 0
        for task_name, cycle, member in self:
            if submission_dirs:
                submission_dir = self.get_submission_dir(task_name, cycle, member, create=False)
                if os.path.exists(submission_dir):
                    self.logger.debug(f"Removing submission directory: {submission_dir}")
                    if not dry:
                        shutil.rmtree(submission_dir)
                    nitems += 1
                    self.logger.info(f"Removed submission directory: {submission_dir}")

            if run_dirs:
                run_dir = self.get_run_dir(task_name, cycle, member)
                if os.path.exists(run_dir):
                    self.logger.debug(f"Removing submission directory: {run_dir}")
                    if not dry:
                        shutil.rmtree(run_dir)
                    nitems += 1
                    self.logger.info(f"Removed submission directory: {run_dir}")

            if artifacts:
                for name, path in self.get_task_artifacts(task_name, cycle, member).items():
                    self.logger.debug(f"Removing '{name}' artifact: {path}")
                    if not dry:
                        os.remove(path)
                    nitems += 1
                    self.logger.info(f"Removed '{name}' artifact: {path}")

        # Log files
        if log_files:
            for ext in "", ".[1-3]":
                for log_file in glob.glob(os.path.join(self.workflow_dir, "log/woom.log")):
                    self.logger.debug(f"Removing log file: {log_file}")
                    if not dry:
                        os.remove(log_file)
                    nitems += 1
                    self.logger.info(f"Removed log file: {log_file}")

        # Extra files and dirs
        if extra_files:
            if isinstance(extra_files, str):
                extra_files = [extra_files]
            for pat in extra_files:
                if not os.path.isabs(pat):
                    pat = os.path.join(self.workflow_dir, pat)
                for extra in glob.glob(pat):
                    if os.path.isdir(extra):
                        self.logger.debug(f"Removing extra directory: {extra}")
                        if not dry:
                            shutil.rmtree(extra)
                        nitems += 1
                        self.logger.info(f"Removed extra directory: {extra}")
                    else:
                        self.logger.debug(f"Removing extra file: {extra}")
                        if not dry:
                            os.remove(extra)
                        nitems += 1
                        self.logger.info(f"Removed extra file: {extra}")

        self.logger.debug("Finished cleaning")
        if nitems:
            self.logger.debug(f"  Removed {nitems} individual file or directories")
        else:
            self.logger.debug("  Nothing to remove")
