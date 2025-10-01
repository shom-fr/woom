#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job management utilities
"""
import datetime
import json
import logging
import os
import subprocess
from enum import Enum

import psutil

from . import util as wutil
from .__init__ import WoomError

# from .env import is_os_cmd_avail

ALLOWED_SCHEDULERS = ["background", "slurm", "pbspro"]

logger = logging.getLogger(__name__)


class WoomJobError(WoomError):
    pass


class JobStatus(Enum):
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

    def is_killed(self):
        return self.name == "KILLED"

    @property
    def jobid(self):
        if not hasattr(self, "_jobid"):
            self._jobid = ""
        return self._jobid

    @jobid.setter
    def jobid(self, jobid):
        self._jobid = jobid


# %% Background processes


class Job:
    """Single job"""

    overview_format = dict(
        name="20",
        jobid="8",
        session="22",
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
        self.submission_date = submission_date
        self.subproc = subproc
        if isinstance(status, str):
            status = JobStatus[status]
        self.status = status
        self.status.jobid = jobid
        self.artifacts = artifacts

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
            json_file = os.path.splitext(self.script)[0] + ".json"
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
        if self.status.is_killed():  # don't query in this case
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

    def kill(self):
        if self.is_running():
            self._get_proc_().kill()
            self.set_status("KILLED")

    def wait(self):
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

    # def __init__(self, session):
    def __init__(self):
        self.jobs = []
        # self.session = session
        # logger.info(f"Started job manager: {self.__class__.__name__}(session='{self.session}')")
        # self.load()
        logger.info(f"Started job manager: {self.__class__.__name__}()")

    def load_job(self, json_file, append=True):
        """Load a single job from its json dump file"""
        return self.job_class.load(self, json_file, append)

    def load(self, json_files):
        """Load jobs from json dump files"""
        for json_file in json_files:
            self.load_job(json_file, append=True)

    def dump(self):
        """Store jobs to session files"""
        for job in self.jobs:
            job.dump()

    def __repr__(self):
        return f"<{self.__class__.__name__}(session={self.session})>"

    @staticmethod
    def from_scheduler(scheduler):
        scheduler = scheduler.lower()
        assert (
            scheduler in ALLOWED_SCHEDULERS
        ), f"Invalid scheduler: {scheduler}. Valid:  {ALLOWED_SCHEDULERS}"
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

    def __contains__(self, job):
        return self.get_job(job) is not None

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
            jobs = self.jobs
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

    def __getitem__(self, jobid):
        return self.get_job(jobid)

    def __str__(self):
        return self.get_overview()

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
                        if isinstance(ovalue, list):
                            ovalue = [val for val in ovalue if val]
                            # if not isinstance(
                            #     cls.commands[command]["options"][oname], tuple
                            # ):
                            #     sep = ","
                            # else:
                            #     fmt, sep = fmt
                            for val in ovalue:
                                args.append(fmt.format(val))
                        else:
                            fmt = fmt.format(ovalue).split()
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
            opts.update(opts.pop("extra"))

        # Format commandline arguments
        return self.get_command_args("submit", **opts)

    def submit(self, script, opts, depend=None, submdir=None, stdout=None, stderr=None, artifacts=None):
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
        if stdout is None:
            stdout = open(f"{rootname}.out", "w")
        if stderr is None:
            stderr = open(f"{rootname}.err", "w")

        # Submit
        logger.debug("Submit: " + " ".join(jobargs))
        subproc = subprocess.Popen(jobargs, stdout=stdout, stderr=stderr, cwd=submdir)
        logger.debug("Submitted")

        # Init Job instance
        job = self.job_class(
            manager=self,
            script=script,
            name=opts.get("name"),
            queue=opts.get("queue"),
            args=subproc.args,
            jobid=str(subproc.pid),
            submission_date=str(datetime.datetime.now())[:-7],
            subproc=subproc,
            artifacts=artifacts,
        )
        job.dump()
        self.jobs.append(job)
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
        """Query status for a single job"""
        args = self.manager._extra_status_args_(self.manager.get_command_args("status", jobid=self.jobid))
        logger.debug("Get status: " + " ".join(args))
        res = subprocess.run(args, capture_output=True, check=True)
        logger.debug("Got status")
        if res.returncode:
            return "UNKNOWN"
        return self.manager._parse_status_res_(res)[0]

    def is_running(self):
        return self.get_status().is_running()

    def wait(self):
        pass

    def kill(self):
        args = self.manager.get_command_args("delete", force="-W force", jobid=self.jobid)
        res = subprocess.run(args, capture_output=True, check=True)
        if not res.returncode:
            self.set_status("KILLED")


class _Scheduler_(BackgroundJobManager):
    job_class = ScheduledJob

    def get_submission_command(self, script, opts, depend=None):
        if depend:
            opts["depend"] = ":".join([str(job) for job in depend])
        return super().get_submission_command(script, opts, depend=depend)

    def submit(self, script, opts, depend=None, submdir=None, stdout=None, stderr=None, artifacts=None):
        """Submit the script and instantiate a :class:`Job` object"""

        # stdout and stderr
        rootname = os.path.splitext(script)[0]
        if stdout is None:
            stdout = f"localhost:{rootname}.out"
        if stderr is None:
            stderr = f"localhost:{rootname}.err"
        opts["log_out"] = stdout
        opts["log_err"] = stderr

        # Submision
        job = super().submit(
            script,
            opts,
            depend=depend,
            submdir=submdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            artifacts=artifacts,
        )
        job.subproc.wait()

        # Post-proc
        stdout = job.subproc.stdout.read().decode("utf-8", errors="ignore")
        stderr = job.subproc.stderr.read().decode("utf-8", errors="ignore")
        logger.debug("Job submit stdout: " + stdout)
        logger.debug("Job submit stderr: " + stderr)
        # if job.subproc.stderr:
        # logger.debug(
        # "Job submit stderr: " + job.subproc.stderr.read().decode("utf-8", errors="ignore")
        # )
        # if job.subproc.stdout:
        # logger.debug(
        # "Job submit stdout: " + job.subproc.stdout.read().decode("utf-8", errors="ignore")
        # )
        if job.subproc.returncode:
            raise WoomJobError(f"Submission failed with error message: {stderr}")
        self._parse_submit_job_(job, stdout)  # update jobid
        job.dump()
        # self.check_status(show=False)
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
                "time": "-l walltime={}",
                "memory": "-l mem={}",
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
                session,
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
                "mem": "--mem={}",
                "time": "--time={}",
                "depend": "--dependency=afterok:{}",
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
        "delete": {
            "command": "scancel",
            "options": {
                "jobid": "{}",
            },
        },
    }

    status_names = {
        "R": JobStatus.RUNNING,
        "CD": JobStatus.FINISHED,
        "PD": JobStatus.PENDING,
        "CG": JobStatus.COMPLETING,
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
        res = super()._parse_status_res_(self, res)
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
