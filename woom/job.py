#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job management utilities
"""
import os

# import contextlib
import logging
import subprocess
from enum import Enum
import datetime
import json

import psutil

from . import util as wutil

# from .env import is_os_cmd_avail

ALLOWED_SCHEDULERS = ["background", "slurm", "pbspro"]

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    UNKOWN = -1
    FINISHED = 0
    PENDING = 1
    RUNNING = 2
    INQUEUE = 3
    EXITING = 4
    COMPLETING = 5


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
        # session=None,
        submission_date=None,
        # submission_dir=None,
        # token=None,
        subproc=None,
    ):
        self.manager = manager
        self.name = name
        self.queue = queue
        self.jobid = jobid
        self.script = script
        self.args = args
        self.status = JobStatus.UNKOWN
        self.realqueue = None
        self.time = None
        self.memory = None
        # self.session = manager.session
        self.submission_date = submission_date
        # self.submission_dir = submission_dir
        # self.token = token
        self.subproc = subproc

    @classmethod
    def load(cls, manager, json_file):
        """Load a job into a manager from a json file"""
        with open(json_file) as jsonf:
            content = json.load(jsonf)
        job = cls(
            manager=manager,
            name=content["name"],
            script=content["script"],
            args=content["args"],
            jobid=content["jobid"],
            queue=content["queue"],
            # session=content["session"],
            submission_date=content["submission_date"],
            # submission_dir=content["submission_dir"],
            # token=content["token"],
        )
        manager.jobs.append(job)

    def dump(self):
        """Export to json in session's cache"""
        jobdict = self.to_dict()
        # if not jobdict["name"] and not jobdict["token"]:
        # logger.warning("Can't dump to json a job with no name or token")
        # return
        # if jobdict["token"]:
        # json_file = jobdict["token"]
        # else:
        # json_file = jobdict["name"]
        # json_file += ".json"
        # with self.session.open_file("jobs", json_file, "w") as f:
        json_file = os.path.splitext(self.script)[0] + ".json"
        # with self.session.open_file("jobs", json_file, "w") as f:
        with open(json_file, "w") as f:
            json.dump(jobdict, f, indent=4, cls=wutil.WoomJSONEncoder)
            json_path = f.name
        # wutil.make_latest(json_path)
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

    def get_status(self):
        """Query the status"""
        try:
            self._get_proc_()
            self.status = JobStatus.RUNNING
        except psutil.NoSuchProcess:
            self.status = JobStatus.UNKOWN
        return self.status

    def update_status(self, status=None):
        """Change de job status of this instance"""
        if status is None:
            status = self.get_status()
        if status is None:
            return
        if isinstance(status, JobStatus):
            self.status = status
        else:  # dict
            self.jobid = status["jobid"]
            self.realqueue = status["queue"]
            self.status = status["status"]
            self.time = status["time"]
        self.dump()

    def is_running(self):
        try:
            p = self._get_proc_()
            return p.is_running()
        except psutil.NoSuchProcess:
            return False

    def kill(self):
        if self.is_running():
            self._get_proc_().kill()

    def wait(self):
        if self.is_running():
            p = self._get_proc_()
            logger.debug(f"Waiting for process to finish: {p.pid}")
            p.wait()
            logger.debug("Ok, finished!")

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
        # session = self.session
        submission_date = self.submission_date
        token = self.token
        if self.time is not None:
            hours = self.time.seconds // 3600
            minutes = (self.time.seconds - hours * 3600) // 60
            time = f"{hours:02}h{minutes:02}"
        else:
            time = "--h--"
        fmt = "    ".join([f"{key!s:{ff}}" for key, ff in self.overview_format.items()])
        return fmt.format(**locals())

    def to_dict(self):
        dict_job = dict()
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

    # def load(self):
    # """Load jobs from session files"""
    # json_files = self.session.get_files("jobs", "*.json")
    # self.jobs = [self.job_class.load(self, json_file) for json_file in json_files]

    def load_job(self, json_file):
        """Load a single job from its json dump file"""
        self.jobs.append(Job.load(self, json_file))

    def load(self, json_files):
        """Load jobs from json dump files"""
        for json_file in json_file:
            self.load_job(json_file)

    def dump(self):
        """Store jobs to session files"""
        for job in self.jobs:
            job.dump()

    def __repr__(self):
        return f"<{self.__class__.__name__}(session={self.session})>"

    @staticmethod
    def from_scheduler(scheduler):  # , session):
        scheduler = scheduler.lower()
        assert (
            scheduler in ALLOWED_SCHEDULERS
        ), f"Invalid scheduler: {scheduler}. Valid:  {ALLOWED_SCHEDULERS}"
        cls_name = scheduler.title() + "JobManager"
        from . import job

        return getattr(job, cls_name)()  # session)

    # def load_json(self, json_files):
    # """Load jobs from json files"""
    # for json_file in json_files:
    # with open(json_file) as jsonf:
    # content = json.load(jsonf)
    # job = Job(
    # manager=self,
    # name=content["name"],
    # args=content["args"],
    # jobid=content["jobid"],
    # queue=content["queue"],
    # session=content["session"],
    # submission_date=content["submission_date"],
    # token=content["token"],
    # )
    # self.jobs.append(job)
    # return self.jobs

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
        jobids: list(str), None
            Explicit list of job ids. `name` and `queue` are ignored when `jobids` is used.
        name: str, None
            Select jobs from name
        queue: str, None
            Select jobs from queue

        Return
        ------
        jobs
            List of :class:`Job` objects
        jobids
            List of job ids
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
                if (name is not None and job.name != name) or (
                    queue is not None and job.queue != queue
                ):
                    continue
                jobs.append(job)
        else:
            jobs = self.jobs
        return jobs

    # def get_jobids(self, name=None, queue=None):
    # """Get job ids

    # Parameters
    # ----------
    # name: str, None
    # Select jobs from name
    # queue: str, None
    # Select jobs from queue

    # Return
    # ------
    # jobs
    # List of :class:`Job` objects
    # jobids
    # List of job ids
    # """
    # if isinstance(name, Job):
    # assert name in self.jobs
    # jobs = [name]
    # else:
    # jobs = self.get_jobs(name=name, queue=queue)
    # jobids = [job.jobid for job in jobs if job.jobid is not None]
    # return jobs, jobids

    def update_status(self, jobids=None, name=None, queue=None):
        """Query status"""
        jobs = self.get_jobs(jobids=jobids, name=name, queue=queue)
        for job in jobs:
            job.update_status()
        return jobs

    def get_status(self, jobids=None, name=None, queue=None):
        """Update and return jobs status

        Return
        ------
        list(Job)
        """
        jobs = self.update_status(jobids=jobids, name=name, queue=queue)
        return [job.status for job in jobs]

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

    def __getitem__(self, name):
        return self.get_jobs(name=name)

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

    def get_submission_args(self, script, opts, depend=None):
        # self.session  opts["session"]
        # script = f"{opts['job']}"

        # opts = self._get_opts_("submit", opts)
        # if "queue" not in opts.keys():
        #     opts.update({"queue": None})

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

    def submit(self, script, opts, depend=None, submdir=None, stdout=".out", stderr=".err"):
        # Wait for dependencies
        if depend:
            for job in depend:
                job.wait()

        # Get submission arguments
        jobargs = self.get_submission_args(script, opts, depend=depend)

        ## Submission directory = where the script is
        # if submdir is None:
        # submdir = os.path.dirname(script)

        # stdout and stderr
        if isinstance(stdout, str):
            if stdout.startswith("."):
                stdout = os.path.splitext(script)[0] + stdout
            stdout = open(stdout, "w")
        if isinstance(stderr, str):
            if stderr.startswith("."):
                stderr = os.path.splitext(script)[0] + stderr
            stderr = open(stderr, "w")

        # Submit
        logger.debug("Submit: " + " ".join(jobargs))
        # res = subprocess.run(jobargs, capture_output=True, check=True)
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
            # session=self.session,
            submission_date=str(datetime.datetime.now())[:-7],
            # submission_dir=submdir,
            # token=opts.get("token"),
            subproc=subproc,
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

    def delete(self):
        self.check_status()
        cond = input("Do you really want to delete the jobs listed hereabove ?(yes/no)")
        if cond == "yes":
            for job in self.jobs:
                job.kill()

    def delete_force(self):
        self.check_status()
        for job in self.jobs:
            job.kill()


# %% With scheduler


class ScheduledJob(Job):
    def get_status(self):
        return self.manager.get_status(self)

    def is_running(self):
        return self.get_status() is JobStatus.RUNNING

    def wait(self):
        pass


class _Scheduler_(BackgroundJobManager):
    job_class = ScheduledJob

    def get_submission_args(self, script, opts, depend=None):
        if depend:
            opts["depend"] = ":".join([str(job) for job in depend])
        return super().get_submission_args(script, opts, depend=depend)

        # # self.session  opts["session"]
        # # script = f"{opts['job']}"

        # # opts = self._get_opts_("submit", opts)
        # # if "queue" not in opts.keys():
        # #     opts.update({"queue": None})

        # # Finalize options
        # opts.update(dict(script=script))
        # if depend:
        #     if isinstance(depend, str):
        #         depend = [depend]
        #     opts["depend"] = ":".join(depend)
        # if "extra " in opts:
        #     opts.update(opts.pop("extra"))

        # # Format commandline arguments
        # return self.get_command_args("submit", **opts)

    def submit(self, script, opts, depend=None, submdir=None):
        """Submit the script and instantiate a :class:`Job` object"""
        job = super().submit(
            script,
            opts,
            depend=depend,
            submdir=submdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        job.subproc.wait()
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
        self._parse_submit_job_(job, stdout)  # update jobid
        job.dump()
        # self.check_status(show=False)
        return job

    def update_status(self, jobids=None, name=None, queue=None):
        """Query status"""
        jobs = self.get_jobids(jobids=jobids, name=name, queue=queue)
        jobids = [job.id for job in jobs]
        args = self._extra_status_args_(
            self.get_command_args("status", jobid=self.jobid_sep.join(jobids))
        )
        if args:
            logger.debug("Get status: " + " ".join(args))
            res = subprocess.run(args, capture_output=True, check=True)
            logger.debug("Got status")
            status_list = self._parse_status_res_(res)
        if status_list:
            for status in status_list:
                self.get_job(status["jobid"]).update_status(status)

    def _parse_status_res_(self, res):
        if res.stderr:
            logger.debug("Job status stderr: " + res.stderr.decode("utf-8", errors="ignore"))
        if res.stdout:
            logger.debug("Job status stdout: " + res.stdout.decode("utf-8", errors="ignore"))
        return res.stdout.decode("utf-8", errors="ignore")

    def delete(self):
        self.update_status()
        cond = input("Do you really want to delete the jobs listed hereabove ?(yes/no)")
        if cond == "yes":
            args = self.get_command_args(
                "delete",
                force="-W force",
                jobid=self.jobid_sep.join([job.jobid for job in self.jobs]),
            )
            print(args)
            subprocess.run(args, capture_output=True, check=True)

    def delete_force(self):
        self.check_status()
        args = self.get_command_args(
            "delete",
            jobid=self.jobid_sep.join([job.jobid for job in self.jobs]),
        )
        subprocess.run(args, capture_output=True, check=True)


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
                "log_out": "-koed -o {}",
                "depend": ("-W depend=afterok:{}"),
                "mail": "-M {}",
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

    # def submit(self, opts):
    #     jobid = BasicJobManager.submit(self, opts)
    #     return jobid

    @staticmethod
    def _parse_submit_job_(job, stdout):
        # job.jobid = job.subproc.stdout.read().decode("utf-8", errors="ignore").split(".")[0]
        job.jobid = stdout.split(".")[0]

    def _extra_status_args_(self, args):
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
            if status in self.status_names:
                status = self.status_names[status]
            else:
                status = JobStatus.UNKOWN
            out.append(
                {
                    "jobid": jobid.split(".")[0],
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

    # def submit(self, opts):
    #     jobid = BasicJobManager.submit(self, opts)
    #     return jobid

    def _extra_status_args_(self, args):
        args.append("--noheader")
        return args

    @staticmethod
    def _parse_submit_job_(job, stdout):
        job.jobid = stdout.split()[-1]
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
                user = info[3]
                status = info[4]
                time = info[5]
                nodes = info[6]
                nodelist = " ".join(info[7:])

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
                    status = JobStatus.UNKOWN
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


# if is_os_cmd_avail("squeue"):
#     jobmanager = SlurmJobManager()
# elif is_os_cmd_avail("qsub"):
#     jobmanager = PbsJobManager()
# else:
#     jobmanager = BasicJobManager()

"""
if __name__ == '__main__':
    jm = BasicJobManager()
    #job = Job(jm, name='toto', args='mkjob.py rundate=10', queue='seq',jobid='007')
    job = Job(jm, name='fake_task', args='fake_task.py rundate=10', queue='None')
    jm.jobs.append(job)
    print(job.get_status())
    #print(job)
    print(Job.get_overview_header())
    #print(job.get_overview())
    #print(jm)
"""
