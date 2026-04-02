#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job management utilities
"""

import datetime
import json
import logging
import os
import shlex
import subprocess
import time
from enum import Enum

import psutil

from . import util as wutil
from .__init__ import WoomError, woom_warn

# from .env import is_os_cmd_avail

ALLOWED_SCHEDULERS = ["background", "slurm", "pbspro"]

logger = logging.getLogger(__name__)


class WoomJobError(WoomError):
    pass


class JobStatus(Enum):
    TERMINATED = -7
    FAILED = -6
    ERROR = -5
    SUCCESS = -4
    KILLED = -3
    NOTSUBMITTED = -2
    FINISHED = -1
    UNKNOWN = 0
    PENDING = 1
    RUNNING = 2
    INQUEUE = 3
    EXITING = 4
    COMPLETING = 5

    def is_running(self):
        return self.value > 0

    def is_not_running(self):
        return self.value < 0

    def is_unknown(self):
        return self.value == 0

    def has_been_canceled(self):
        return self.name in ["KILLED", "TERMINATED"]

    def has_failed(self):
        return self.name in ["ERROR", "FAILED", "KILLED"]

    @property
    def jobid(self):
        if not hasattr(self, "_jobid"):
            self._jobid = ""
        return self._jobid

    @jobid.setter
    def jobid(self, jobid):
        self._jobid = str(jobid)


# %% Background processes


class Job:
    """Single job

    Parameters
    ----------
    manager : JobManager
        Job manager instance
    name : str
        Job name
    script : str
        Path to job script
    args : list
        Command line arguments
    queue : str, optional
        Queue name
    jobid : str, optional
        Job identifier
    submission_date : str, optional
        Date of submission
    status : str or JobStatus, optional
        Job status
    subproc : subprocess.Popen, optional
        Subprocess object for background jobs
    artifacts : dict, optional
        Job artifacts
    submission_dir : str, optional
        Submission directory path
    blocking : bool, optional
        Whether job is blocking
    """

    overview_format = dict(
        name="20",
        jobid="8",
        queue="10",
        realqueue="10",
        time="5",
        status="10",
        submission_date="20",
        # token="70",
        script="120",
    )

    def __init__(
        self,
        manager,
        name,
        script,
        args,
        queue=None,
        jobid=None,
        submission_date=None,
        status="UNKNOWN",
        subproc=None,
        artifacts=None,
        submission_dir=None,
        blocking=True,
    ):
        self.manager = manager
        self.name = name
        self.queue = queue
        self.jobid = jobid
        self.script = script
        self.args = args
        self.realqueue = None
        self.time = None
        self.memory = None
        self.submission_date = submission_date if submission_date else str(datetime.datetime.now())[:-7]
        if script is not None and submission_dir is None:
            submission_dir = os.path.dirname(script)
        self.submission_dir = submission_dir
        self.subproc = subproc
        if isinstance(status, str):
            status = JobStatus[status]
        self.status = status
        self.status.jobid = jobid
        self.artifacts = artifacts
        self.blocking = blocking

    @classmethod
    def load(cls, manager, json_file, append=True):
        """Load a job into a manager from a json file"""
        with open(json_file) as jsonf:
            content = json.load(jsonf)
        if manager.__class__.__name__ != content["manager"]:
            raise WoomJobError(f"Cannot load this job in a {manager.__class__.__name__} manager: {json_file}")
        job = cls(
            manager=manager,
            name=content["name"],
            script=content["script"],
            args=content["args"],
            jobid=content["jobid"],
            queue=content["queue"],
            status=content["status"],
            submission_date=content["submission_date"],
            artifacts=content.get("artifacts"),
            blocking=content.get("blocking", True),
        )
        if append and content["jobid"] not in manager:
            manager.jobs.append(job)
        return job

    def to_dict(self):
        dict_job = {}
        for key, value in self.__dict__.items():
            if isinstance(value, str):
                if value != "":
                    dict_job[key] = value
            elif key == "manager":
                dict_job[key] = value.__class__.__name__
            elif key == "status":
                dict_job[key] = str(value.name)
            elif key == "time":
                if value is not None:
                    hours = value.seconds // 3600
                    minutes = (value.seconds - hours * 3600) // 60
                    dict_job[key] = f"{hours:02}h{minutes:02}"
                else:
                    dict_job[key] = "--h--"
            else:
                dict_job[key] = value

        return dict_job

    def dump(self, json_file=None):
        """Export to json in job script directory"""
        jobdict = self.to_dict()
        if json_file is None:
            json_file = self.files["json"]
        with open(json_file, "w") as f:
            json.dump(jobdict, f, indent=4, cls=wutil.WoomJSONEncoder)
            json_path = f.name
        return json_path

    def __str__(self):
        return self.jobid

    def __repr__(self):
        return "<Job(name={}, status={}, jobid={}, script={})>".format(
            self.name, self.status.name, self.jobid, self.script
        )

    def __eq__(self, job):
        return str(self) == str(job)

    def __hash__(self):
        return hash(str(self))

    @property
    def files(self):
        """:class:`dict` of job files like script, status, out, err and json"""
        submdir = os.path.dirname(self.script)
        return {
            "script": self.script,
            "status": os.path.join(submdir, "job.status"),
            "err": os.path.join(submdir, "job.err"),
            "out": os.path.join(submdir, "job.out"),
            "json": os.path.join(submdir, "job.json"),
            "terminating": os.path.join(submdir, "job.terminating"),
        }

    def _get_proc_(self):
        if isinstance(self.jobid, subprocess.Popen):
            pid = self.jobid.pid
        else:
            pid = self.jobid
        return psutil.Process(int(pid))

    def query_status(self):
        """Query the status

        .. warning:: It does not update the status! It is just a query.
        """
        try:
            proc = self._get_proc_()
            status = JobStatus.RUNNING
            status.jobid = str(proc.pid)
        except psutil.NoSuchProcess:
            status = JobStatus.UNKNOWN
            status.jobid = self.jobid
        return status

    def get_status(self, fallback=None):
        """Query and set the status of this job"""
        if self.status.has_been_canceled():  # don't query in this case
            return self.status
        return self.set_status(self.query_status(), fallback=fallback)

    def set_status(self, status, fallback=None):
        """Set the status of this job without query

        Don't update with unknown state if the job is supposed to be finished.
        """
        if isinstance(status, str):
            status = JobStatus[status.upper()]
        if isinstance(status, JobStatus):
            status = status
            status.jobid = self.jobid
        else:  # dict
            assert self.jobid == status["jobid"]
            dstatus = status
            self.realqueue = dstatus["queue"]
            status = dstatus["status"]
            status.jobid = dstatus["jobid"]
            self.time = dstatus["time"]
        status.jobid = self.jobid

        if status.is_unknown() and self.status.is_not_running():
            return self.status

        self.status = status
        self.dump()
        return self.status

    def is_running(self):
        try:
            p = self._get_proc_()
            return p.is_running()
        except psutil.NoSuchProcess:
            return False

    def kill(self, graceful=True, timeout=10):
        """Kill the job, optionally trying graceful termination first

        Parameters
        ----------
        graceful: bool
            If True, send SIGTERM first and wait for graceful shutdown
        timeout: int
            Seconds to wait for graceful shutdown before forcing SIGKILL
        """
        if self.is_running():
            proc = self._get_proc_()

            if graceful:
                # Try graceful termination first
                logger.debug(f"Sending SIGTERM to process: {proc.pid}")
                proc.terminate()  # Sends SIGTERM

                try:
                    # Wait for the process to terminate gracefully
                    proc.wait(timeout=timeout)
                    logger.debug(f"Process {proc.pid} terminated gracefully")
                    # The script's on_term handler wrote status 0
                    self.set_status("SUCCESS")
                    return
                except psutil.TimeoutExpired:
                    logger.warning(f"Process {proc.pid} did not terminate gracefully, forcing kill")

            # Force kill with SIGKILL
            proc.kill()

            # Since SIGKILL can't be trapped, we must write the status file ourselves
            status_file = self.files["status"]
            exit_code = "0" if graceful else "1"
            logger.debug(f"Writing exit status to {status_file}: {exit_code}")
            with open(status_file, 'w') as f:
                f.write(exit_code)
            self.set_status("TERMINATED" if graceful else "KILLED")

    def wait(self):
        """Wait for a job to finish"""
        if self.is_running():
            p = self._get_proc_()
            logger.debug(f"Waiting for process to finish: {p.pid}")
            exit_status = p.wait()
            if exit_status:
                logger.error(f"Finished with exit status: {exit_status}")
            else:
                logger.debug("Ok, finished!")
            return exit_status

    @classmethod
    def get_overview_header(cls):
        heads = []
        tails = []
        for name, fmt in cls.overview_format.items():
            name = name.upper()
            heads.append(f"{name:{fmt}}")
            tails.append("-" * len(heads[-1]))
        return "      ".join(heads) + "\n" + "    ".join(tails)

    def get_overview(self, update=True):
        # TODO: Must be converted to use pandas
        if update:
            self.update_status()
        name = self.name
        jobid = self.jobid
        queue = self.queue
        realqueue = self.realqueue
        status = self.status.name
        submission_date = self.submission_date
        if self.time is not None:
            hours = self.time.seconds // 3600
            minutes = (self.time.seconds - hours * 3600) // 60
            time = f"{hours:02}h{minutes:02}"
        else:
            time = "--h--"
        fmt = "    ".join([f"{key!s:{ff}}" for key, ff in self.overview_format.items()])
        return fmt.format(**locals())


class BackgroundJobManager(object):
    """Manager for jobs that run in background"""

    with_scheduler = False

    commands = {
        "submit": {
            "command": "bash",
            "options": {
                "script": "{}",
                "log_out": "-o {}",
            },
        },
    }

    status_names = {
        "F": JobStatus.FINISHED,
    }

    job_class = Job

    def __init__(self):
        self.jobs = []
        logger.info(f"Started job manager: {self.__class__.__name__}()")

    def load_job(self, json_file, append=True):
        """Load a single job from its json dump file"""
        return self.job_class.load(self, json_file, append)

    def load(self, json_files):
        """Load jobs from json dump files"""
        for json_file in json_files:
            self.load_job(json_file, append=True)

    def dump(self):
        """Store jobs to json files"""
        for job in self.jobs:
            job.dump()

    # def __repr__(self):
    #     return f"<{self.__class__.__name__}>"

    @staticmethod
    def from_scheduler(scheduler):
        scheduler = scheduler.lower()
        assert scheduler in ALLOWED_SCHEDULERS, (
            f"Invalid scheduler: {scheduler}. Valid:  {ALLOWED_SCHEDULERS}"
        )
        cls_name = scheduler.title() + "JobManager"
        from . import job

        return getattr(job, cls_name)()

    def get_job(self, jobid):
        """Get :class:`Job` from id"""
        jobid = str(jobid)
        if jobid is None:
            return
        for job in self.jobs:
            if job.jobid == jobid:
                return job

    def get_jobs(self, jobids=None, name=None, queue=None):
        """Get job ids

        Parameters
        ----------
        jobids: list(str), str, None
            Explicit list of job ids. `name` and `queue` are ignored when `jobids` is used.
        name: str, None
            Select jobs from name
        queue: str, None
            Select jobs from queue

        Return
        ------
        list(Job)
            List of :class:`Job` objects
        """
        jobs = []
        if jobids:
            if not isinstance(jobids, list):
                jobids = [jobids]
            for job in self.jobs:
                for jobid in jobids:
                    if job.id == str(jobid):
                        jobs.append(job)
        elif name:
            for job in self.jobs:
                if job.name.lower() == name.lower():
                    jobs.append(job)
        elif queue:
            for job in self.jobs:
                if (name is not None and job.name != name) or (queue is not None and job.queue != queue):
                    continue
                jobs.append(job)
        else:
            jobs = list(self.jobs)
        return jobs

    def get_status(self, jobids=None, name=None, queue=None, fallback=None):
        """Update and return jobs status

        Return
        ------
        list(Job)
        """
        jobs = self.get_jobs(jobids=jobids, name=name, queue=queue)
        return [job.get_status(fallack=fallback) for job in jobs]

    def set_status(self, jobids=None, name=None, queue=None, fallback=None):
        """Query status"""
        jobs = self.get_jobs(jobids=jobids, name=name, queue=queue)
        return [job.set_status(fallback=fallback) for job in jobs]

    def get_overview(self, jobids=None, name=None, queue=None):
        jobs = self.update_status(jobids=jobids, name=name, queue=queue)
        header = Job.get_overview_header()
        overviews = [job.get_overview(update=False) for job in jobs]
        return header + "\n" + "\n".join(overviews)

    def check_status(self, show=True):
        """Update jobs status and show them"""
        overview = self.get_overview()
        if show:
            print(overview)

    def __contains__(self, job):
        return self.get_job(job) is not None

    def __getitem__(self, jobid):
        return self.get_job(jobid)

    def drop(self, job):
        for j in list(self.jobs):
            if j == job:
                self.jobs.remove(j)
                return
        raise WoomJobError(f"Can't drop job from manager: {job}")

    def __delitem__(self, jobid):
        self.drop(jobid)

    def __str__(self):
        # return self.get_overview()
        return self.__class__.__name__

    def __repr__(self):
        return f"<{self}"

    @classmethod
    def get_command_args(cls, command, **opts):
        """Convert commandline specifcations and values to a list of arguments

        Parameters
        ----------
        command: str
            A valid key of the :attr:`commands` dictionary attribute
        kwargs: dict
            Dictionary to fill patterns defined in the :attr:`commands`
            attribute.

        Return
        ------
        list
        """
        args = []
        if "command" in cls.commands[command]:
            args.append(cls.commands[command]["command"])
        if "options" in cls.commands[command]:
            for oname, ovalue in opts.items():
                if oname in cls.commands[command]["options"]:
                    if ovalue is not None:
                        fmt = cls.commands[command]["options"][oname]
                        if isinstance(ovalue, bool):
                            args += shlex.split(fmt)
                        elif isinstance(ovalue, list):
                            ovalue = [val for val in ovalue if val]
                            for val in ovalue:
                                args += shlex.split(fmt.format(val))
                        else:
                            fmt = shlex.split(fmt.format(ovalue))
                            args += fmt
        return args

    def get_submission_command(self, script, opts, depend=None):
        # Finalize options
        opts.update(dict(script=script))
        # if depend:
        #     if isinstance(depend, str):
        #         depend = [depend]
        #     opts["depend"] = ":".join(depend)

        if "extra " in opts:
            woom_warn("The 'extra' submission option is deprecated", "deprecation")
            opts.update(opts.pop("extra"))

        # Format commandline arguments
        return self.get_command_args("submit", **opts)

    def create_job(self, script=None, name=None, args=[], fake=False, **kwargs):
        """Quickly create a job instance and add it to the manager"""
        job = self.job_class(manager=self, script=script, name=name, args=args, **kwargs)
        self.jobs.append(job)
        return job

    def submit(
        self, script, opts, depend=None, submdir=None, stdout=None, stderr=None, artifacts=None, blocking=True
    ):
        # Wait for dependencies
        if depend:
            status = None
            for job in depend:
                status = job.wait()
                if status:
                    logger.error(f"Can't submit job because one of the parent job failed: {job}")
                    return

        # Get submission arguments
        jobargs = self.get_submission_command(script, opts, depend=depend)

        # Submission directory = where the script is
        if submdir is None:
            submdir = os.path.dirname(script)

        # stdout and stderr
        rootname = os.path.splitext(script)[0]
        _owned_stdout = stdout is None
        _owned_stderr = stderr is None
        if stdout is None:
            stdout = open(f"{rootname}.out", "w")
        if stderr is None:
            stderr = open(f"{rootname}.err", "w")

        # Submit
        logger.debug("Submit: " + " ".join(jobargs))
        subproc = subprocess.Popen(jobargs, stdout=stdout, stderr=stderr, cwd=submdir)
        if _owned_stdout:
            stdout.close()
        if _owned_stderr:
            stderr.close()
        logger.debug("Submitted")

        # Init Job instance
        job = self.create_job(
            script=script,
            name=opts.get("name"),
            queue=opts.get("queue"),
            args=subproc.args,
            jobid=str(subproc.pid),
            subproc=subproc,
            artifacts=artifacts,
            submission_dir=submdir,
            blocking=blocking,
        )
        job.dump()
        return job

    def _parse_status_res_(self, res):
        if res.stderr:
            logger.debug("Job status stderr: " + res.stderr.decode("utf-8", errors="ignore"))
        if res.stdout:
            logger.debug("Job status stdout: " + res.stdout.decode("utf-8", errors="ignore"))
        return res.stdout.decode("utf-8", errors="ignore")

    def kill(self, jobids=None, name=None, queue=None):
        for job in self.get_jobs(jobids=jobids, name=name, queue=queue):
            job.kill()
        # cond = input("Do you really want to delete the jobs listed hereabove ?(yes/no)")
        # if cond == "yes":
        #     for job in self.jobs:
        #         job.kill()

    delete = kill


# %% With scheduler


class ScheduledJob(Job):
    def query_status(self):
        """Query status for a single job

        First tries the active jobs queue, then falls back to history if available.
        """
        args = self.manager._extra_status_args_(self.manager.get_command_args("status", jobid=self.jobid))
        logger.debug("Get status: " + " ".join(args))
        # check=False: squeue exits with code 1 when the job is no longer in the active
        # queue (completed/failed). We must not raise — fall through to sacct instead.
        res = subprocess.run(args, capture_output=True, check=False)
        logger.debug("Got status")

        # Parse active jobs (only if the scheduler command succeeded)
        if not res.returncode:
            status_list = self.manager._parse_status_res_(res)
            if status_list:
                return status_list[0]["status"]

        # Fallback to history if job not in active queue
        if hasattr(self.manager, '_query_history_status_'):
            logger.debug("Job not in active queue, checking history")
            return self.manager._query_history_status_(self.jobid)

        return JobStatus.UNKNOWN

    def is_running(self):
        return self.get_status().is_running()

    def wait(self):
        pass

    def kill(self, graceful=True, timeout=10):
        """Kill the job using scheduler commands

        Parameters
        ----------
        graceful: bool
            If True, send SIGTERM and wait before forcing SIGKILL
        timeout: int
            Seconds to wait for graceful shutdown before forcing
        """
        if graceful:
            # Send SIGTERM using scheduler command
            logger.debug(f"Sending SIGTERM to job: {self.jobid}")
            args = self.manager.get_command_args("delete", force=False, terminate=True, jobid=self.jobid)
            res = subprocess.run(args, capture_output=True)

            if res.returncode == 0:
                # Poll status to wait for graceful termination
                start_time = time.time()
                while time.time() - start_time < timeout:
                    status = self.query_status()
                    if status.is_not_running():
                        logger.debug(f"Job {self.jobid} terminated gracefully")
                        # Let get_task_status in workflow.py read the status file
                        return
                    time.sleep(1)

                logger.warning(f"Job {self.jobid} did not terminate gracefully, forcing kill")

        # Force kill with SIGKILL
        logger.debug(f"Forcing kill of job: {self.jobid}")
        args = self.manager.get_command_args("delete", force=True, terminate=False, jobid=self.jobid)
        res = subprocess.run(args, capture_output=True, check=True)
        if res.returncode == 0:
            # SIGKILL won't trigger bash handlers, write status ourselves
            status_file = self.files["status"]
            exit_code = "0" if graceful else "1"
            logger.debug(f"Writing exit status to {status_file}: {exit_code}")
            with open(status_file, 'w') as f:
                f.write(exit_code)
            self.set_status("TERMINATED" if graceful else "KILLED")

    cancel = kill


class _Scheduler_(BackgroundJobManager):
    job_class = ScheduledJob
    with_scheduler = True

    def get_submission_command(self, script, opts, depend=None):
        if depend:
            opts["depend"] = ":".join([str(job) for job in depend])
        return super().get_submission_command(script, opts, depend=depend)

    def submit(
        self, script, opts, depend=None, submdir=None, stdout=None, stderr=None, artifacts=None, blocking=True
    ):
        """Submit the script and instantiate a :class:`Job` object"""

        # stdout and stderr
        # Only set log paths if not already provided by the caller.
        # Using setdefault() preserves absolute paths passed via opts; unconditionally
        # overwriting them caused sbatch to resolve relative names against an unexpected
        # CWD and create spurious log directories.
        rootname = os.path.splitext(os.path.basename(script))[0]
        opts.setdefault("log_out", stdout or f"{rootname}.out")
        opts.setdefault("log_err", stderr or f"{rootname}.err")

        # Submision
        job = super().submit(
            script,
            opts,
            depend=depend,
            submdir=submdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            artifacts=artifacts,
            blocking=blocking,
        )
        job.subproc.wait()

        # Post-proc
        stdout = job.subproc.stdout.read().decode("utf-8", errors="ignore")
        stderr = job.subproc.stderr.read().decode("utf-8", errors="ignore")
        logger.debug("Job submit stdout: " + stdout)
        logger.debug("Job submit stderr: " + stderr)
        if job.subproc.returncode:
            raise WoomJobError(f"Submission failed with error message: {stderr}")
        self._parse_submit_job_(job, stdout)  # update jobid
        job.dump()
        return job

    def _parse_status_res_(self, res):
        if res.stderr:
            logger.debug("Job status stderr: " + res.stderr.decode("utf-8", errors="ignore"))
        if res.stdout:
            logger.debug("Job status stdout: " + res.stdout.decode("utf-8", errors="ignore"))
        return res.stdout.decode("utf-8", errors="ignore")


class PbsproJobManager(_Scheduler_):
    """Pbspro Job Manager"""

    commands = {
        "submit": {
            "command": "qsub",
            "options": {
                "script": "{}",
                "name": "-N {}",
                "queue": "-V -q {}",
                "nnodes": "-l select={}",
                "ncpus": "-l ncpus={}",
                "ngpus": "-l ngpus={}",
                "memory": "-l mem={}",
                "pmem": "-l pmem={}",
                "time": "-l walltime={}",
                "log_out": "-o {}",
                "log_err": "-e {}",
                "depend": ("-W depend=afterok:{}"),
                "mail": "-M {}",
                "extra": "-keod",
            },
        },
        "status": {
            "command": "qstat",
            "options": {
                "jobid": "{}",
                "logname": "-u $LOGNAME",
            },
        },
        "delete": {
            "command": "qdel",
            "options": {
                "force": "-W force",
                "jobid": "{}",
            },
        },
    }

    status_names = {
        "R": JobStatus.RUNNING,
        "F": JobStatus.FINISHED,
        "E": JobStatus.EXITING,
        "Q": JobStatus.INQUEUE,
        "H": JobStatus.PENDING,
        "C": JobStatus.SUCCESS,  # Completed
        "X": JobStatus.SUCCESS,  # Finished successfully
    }

    jobid_sep = " "

    @staticmethod
    def _parse_submit_job_(job, stdout):
        # job.jobid = job.subproc.stdout.read().decode("utf-8", errors="ignore").split(".")[0]
        job.jobid = stdout.split(".")[0]
        job.status.jobid = job.jobid

    def _extra_status_args_(self, args):
        "useful?"
        args.append("-x")
        args.append("-u $LOGNAME")
        return args

    def _parse_status_res_(self, res):
        """JOBID PARTITION NAME USER ST TIME NODES NODELIST(REASON)"""
        res = super()._parse_status_res_(res)
        lines = res.splitlines()[5:]
        out = []
        for line in lines:
            (
                jobid,
                user,
                queue,
                name,
                _,  # session?
                nodes,
                task,
                mem,
                time,
                status,
                elaptime,
            ) = line.split()
            if (elaptime == "--:--") or elaptime == "--":
                elaptime = None
            else:
                hms = elaptime.split(":")
                hh = 0
                mm = 0
                ss = 0
                if len(hms) == 1:
                    ss = hms[0]
                elif len(hms) == 2:
                    hh, mm = hms
                else:
                    hh, mm, ss = hms
                elaptime = datetime.timedelta(seconds=int(ss), minutes=int(mm), hours=int(hh))
            jobid = jobid.split(".")[0]
            if status in self.status_names:
                status = self.status_names[status]
            else:
                status = JobStatus.UNKNOWN
            status.jobid = jobid
            out.append(
                {
                    "jobid": jobid,
                    "queue": queue,
                    "name": name,
                    "time": elaptime,
                    "status": status,
                }
            )
        return out

    @staticmethod
    def get_killed(content):
        """Check the terminated status from job output"""
        if "PBS: job killed: walltime" in content and "Terminated" in content:
            status = JobStatus["FAILED"]
            return status


class SlurmJobManager(_Scheduler_):
    """Slurm Job Manager"""

    commands = {
        "submit": {
            "command": "sbatch",
            "options": {
                "name": "--exclusive -J {}",
                "queue": "-p {}",
                "nnodes": "-N {}",
                "ncpus": "-c {}",
                "ngpus": "--gpus={}",
                "mem": "--mem={}",
                "pmem": "--mem-per-cpu={0} --mem-per-gpu={0}",
                "time": "--time={}",
                "depend": "--dependency=afterok:{} --kill-on-invalid-dep=yes",
                "log_out": "-o {}",
                "log_err": "-e {}",
                "script": "{}",
                "mail": "--mail-type=ALL --mail-user={}",
            },
        },
        "status": {
            "command": "squeue",
            "options": {
                "jobid": "--jobs={}",
                "queue": "--partition={}",
                "name": "--name={}",
                "users": "--users={}",
                "noheader": "--noheader",
            },
        },
        "history": {
            "command": "sacct",
            "options": {
                "jobid": "-j {}",
                "format": "--format=JobID,State,Elapsed",
                "parsable": "--parsable2",
                "noheader": "--noheader",
            },
        },
        "delete": {
            "command": "scancel",
            "options": {
                "jobid": "{}",
                "force": "--signal=KILL",
                "terminate": "--signal=TERM",
            },
        },
    }

    status_names = {
        "R": JobStatus.RUNNING,
        "CD": JobStatus.FINISHED,
        "PD": JobStatus.PENDING,
        "CG": JobStatus.COMPLETING,
        "COMPLETED": JobStatus.SUCCESS,
        "FAILED": JobStatus.FAILED,
        "CANCELLED": JobStatus.KILLED,
        "TIMEOUT": JobStatus.FAILED,
        "OUT_OF_MEMORY": JobStatus.FAILED,
        "NODE_FAIL": JobStatus.FAILED,
        "DEADLINE": JobStatus.FAILED,
    }

    # sacct returns full-word state names (different from squeue short codes).
    history_status_names = {
        "COMPLETED": JobStatus.SUCCESS,
        "FAILED": JobStatus.FAILED,
        "CANCELLED": JobStatus.KILLED,
        "TIMEOUT": JobStatus.FAILED,
        "OUT_OF_MEMORY": JobStatus.FAILED,
        "NODE_FAIL": JobStatus.FAILED,
        "BOOT_FAIL": JobStatus.FAILED,
        "DEADLINE": JobStatus.FAILED,
        "PREEMPTED": JobStatus.FAILED,
        "RUNNING": JobStatus.RUNNING,
        "PENDING": JobStatus.PENDING,
        "SUSPENDED": JobStatus.PENDING,
    }

    jobid_sep = ","

    def _extra_status_args_(self, args):
        args.append("--noheader")
        return args

    @staticmethod
    def _parse_submit_job_(job, stdout):
        job.jobid = stdout.split()[-1]
        job.status.jobid = job.jobid
        # job.jobid = job.subproc.stdout.read().decode("utf-8", errors="ignore").split()[-1]

    def _parse_status_res_(self, res):
        """JOBID PARTITION NAME USER ST TIME NODES NODELIST(REASON)"""
        res = super()._parse_status_res_(res)
        out = []
        lines = res.splitlines()
        if lines:
            for line in lines:
                info = line.split()
                jobid = info[0]
                queue = info[1]
                name = info[2]
                _ = info[3]  # user
                status = info[4]
                time = info[5]
                _ = info[6]  # nodes
                _ = " ".join(info[7:])  # nodelist

                hms = time.split(":")
                hh = 0
                mm = 0
                if len(hms) == 1:
                    ss = hms[0]
                elif len(hms) == 2:
                    mm, ss = hms
                else:
                    hh, mm, ss = hms
                time = datetime.timedelta(seconds=int(mm), minutes=int(hh))
                if status in self.status_names:
                    status = self.status_names[status]
                else:
                    status = JobStatus.UNKNOWN
                status.jobid = jobid
                out.append(
                    {
                        "jobid": jobid,
                        "queue": queue,
                        "name": name,
                        "time": time,
                        "status": status,
                    }
                )
        return out

    def _query_history_status_(self, jobid):
        """Query job status from sacct history

        Parameters
        ----------
        jobid : str
            Job ID to query

        Returns
        -------
        JobStatus
            Status from history or UNKNOWN if not found
        """
        args = self.get_command_args(
            "history",
            jobid=jobid,
            format=True,
            parsable=True,
            noheader=True,
        )
        logger.debug("Query history: " + " ".join(args))

        try:
            res = subprocess.run(args, capture_output=True, check=True, timeout=5)
            if res.returncode or not res.stdout:
                return JobStatus.UNKNOWN

            # Parse: JobID|State|Elapsed
            lines = res.stdout.decode("utf-8", errors="ignore").strip().split("\n")
            if not lines or not lines[0]:
                return JobStatus.UNKNOWN

            # Take first line (main job, not job steps like 12345.0)
            for line in lines:
                if not line or '.batch' in line or '.extern' in line:
                    continue

                parts = line.split("|")
                if len(parts) >= 2:
                    # Extract just the job ID without steps
                    line_jobid = parts[0].split(".")[0]
                    if line_jobid != str(jobid):
                        continue

                    # Remove details like "CANCELLED by 12345"
                    state = parts[1].split()[0]
                    status = self.history_status_names.get(state, JobStatus.FINISHED)
                    status.jobid = jobid
                    logger.debug(f"Found job {jobid} in history with state: {state}")
                    return status

        except subprocess.TimeoutExpired:
            logger.warning(f"Timeout querying job history for {jobid}")
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to query job history for {jobid}: {e}")
        except Exception as e:
            logger.warning(f"Error parsing sacct output for {jobid}: {e}")

        return JobStatus.UNKNOWN

    @staticmethod
    def get_killed(content):
        """Check the terminated status from job output"""
        if ("DUE TO TIME LIMIT" in content or "CANCELLED AT" in content) or (
            "OUT OF MEMORY" in content or ("slurmstepd: error:" in content and "Killed process" in content)
        ):
            status = JobStatus["FAILED"]
            return status
