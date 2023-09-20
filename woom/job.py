#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job management utilities
"""
import logging
import subprocess
from enum import Enum
import datetime

# from .env import is_os_cmd_avail
import json

ALLOWED_SCHEDULERS = ["basic", "slurm", "pbspro"]

logger = logging.getLogger(__name__)


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
    )

    def __init__(
        self,
        manager,
        name,
        args,
        queue=None,
        jobid=None,
        session=None,
        submission_date=None,
    ):
        self.manager = manager
        self.name = name
        self.queue = queue
        self.jobid = jobid
        self.args = args
        self.status = JobStatus.UNKOWN
        self.realqueue = None
        self.time = None
        self.memory = None
        self.session = session
        self.submission_date = submission_date

    def __str__(self):
        return "Job(name={}, status={}, jobid={}, session={})".format(
            self.name, self.status.name, self.jobid, self.session
        )

    def __repr__(self):
        return "<{}>".format(self)

    def get_status(self):
        return self.manager.get_status(self)  # or JobStatus.FINISHED

    def is_running(self):
        return self.get_status() is JobStatus.RUNNING

    def update_status(self, status=None):
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
        session = self.session
        submission_date = self.submission_date
        if self.time is not None:
            hours = self.time.seconds // 3600
            minutes = (self.time.seconds - hours * 3600) // 60
            time = f"{hours:02}h{minutes:02}"
        else:
            time = "--h--"
        fmt = "    ".join(
            [
                ("{" + key + f"!s:{ff}" + "}")
                for key, ff in self.overview_format.items()
            ]
        )
        return fmt.format(**locals())


class JobStatus(Enum):
    UNKOWN = -1
    FINISHED = 0
    PENDING = 1
    RUNNING = 2
    INQUEUE = 3
    EXITING = 4
    COMPLETING = 5


class BasicJobManager(object):
    """Basic job manager"""

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

    def __init__(self, session):
        self.jobs = []
        self.session = session
        logger.info(
            f"Started job manager: {self.__class__.__name__}(session={self.session})"
        )

    def __repr__(self):
        return f"<{self.__class__.__name__}(session={self.session})>"

    @staticmethod
    def from_scheduler(scheduler, session):
        scheduler = scheduler.lower()
        assert scheduler in ALLOWED_SCHEDULERS
        cls_name = scheduler.title() + "JobManager"
        from . import job

        return getattr(job, cls_name)(session)

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

    # @classmethod
    # def _get_opts_(cls, command, opts_in):
    #     sep = ","
    #     opts = {}
    #     if cls.commands[command]["options"]:
    #         for context in cls.commands[command]["options"].keys():
    #             try:
    #                 value = opts_in[context]
    #                 if sep in value:
    #                     value = value.split(sep)
    #             except:
    #                 continue
    #             else:
    #                 opts[context] = value
    #     return opts

    def _parse_submit_res_(self, res, jobargs):
        if res.stderr:
            logger.debug(
                "Job submit stderr: "
                + res.stderr.decode("utf-8", errors="ignore")
            )
        if res.stdout:
            logger.debug(
                "Job submit stdout: "
                + res.stdout.decode("utf-8", errors="ignore")
            )
        job = Job(
            manager=self,
            name=jobargs["name"],
            queue=jobargs["queue"],
            args=res.args,
            jobid=None,
            session=self.session,
            submission_date=str(datetime.datetime.now())[:-7],
        )
        return job

    # def _get_session_id_(self):
    #     return secrets.token_hex(8)

    def to_json(self, job):
        jobdict = self.to_dict(job)
        with self.session.open_file(
            "jobs", jobdict["name"] + ".json", "w"
        ) as f:
            json.dump(jobdict, f, indent=4)

    def from_json(self, json_files):
        for json_file in json_files:
            with open(json_file) as jsonf:
                content = json.load(jsonf)
            job = Job(
                manager=self,
                name=content["name"],
                args=content["args"],
                jobid=content["jobid"],
                queue=content["queue"],
                session=content["session"],
                submission_date=content["submission_date"],
            )
            self.jobs.append(job)
        return self.jobs

    def to_dict(self, job):
        dict_job = dict()
        for key, value in job.__dict__.items():
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

    def _parse_status_res_(self, res):
        if res.stderr:
            logger.debug(
                "Job status stderr: "
                + res.stderr.decode("utf-8", errors="ignore")
            )
        if res.stdout:
            logger.debug(
                "Job status stdout: "
                + res.stdout.decode("utf-8", errors="ignore")
            )
        return res.stdout.decode("utf-8", errors="ignore")

    def get_jobids(self, name=None, queue=None):
        if isinstance(name, Job):
            assert name in self.jobs
            jobs = [name]
        else:
            jobs = self.get_jobs(name=name, queue=queue)
        jobids = [job.jobid for job in jobs if job.jobid is not None]
        return jobs, jobids

    def update_status(self, name=None, queue=None, jobids=None):
        if self.__class__.__name__ != "BasicJobManager":
            if jobids is None:
                jobs, jobids = self.get_jobids(name=name, queue=queue)
            args = self._extra_status_args_(
                self._get_command_args_(
                    "status", jobid=self._jobid_sep_().join(jobids)
                )
            )
            if args:
                logger.debug("Get status: " + " ".join(args))
                res = subprocess.run(args, capture_output=True, check=True)
                logger.debug("Got status")
                status_list = self._parse_status_res_(res)
            if status_list:
                for status in status_list:
                    self.get_job(status["jobid"]).update_status(status)
        else:
            [job.update_status(JobStatus.FINISHED) for job in self.jobs]

    def get_status(self, name=None, queue=None):
        jobs = self.update_status(name=name, queue=queue)
        if jobs:
            if not isinstance(jobs, list):
                return jobs.status
            return [job.status for job in jobs]

    def get_job(self, jobid):
        if jobid is None:
            return
        for job in self.jobs:
            if job.jobid == jobid:
                return job

    def get_jobs(self, name=None, queue=None):
        jobs = []
        if name:
            if len(name) > 1:
                for job in self.jobs:
                    if job.name in name:
                        jobs.append(job)
        else:
            for job in self.jobs:
                if (name is not None and job.name != name) or (
                    queue is not None and job.queue != queue
                ):
                    continue
                jobs.append(job)
        return jobs

    def _get_jobs_session_(self):
        json_files = self.session.get_file("jobs", "*.json")
        self.from_json(json_files)

    def __getitem__(self, name):
        return self.get_jobs(name=name)

    def get_overview(self, jobids=None):
        self.update_status(jobids=jobids)
        header = Job.get_overview_header()
        overviews = [job.get_overview(update=False) for job in self.jobs]
        return header + "\n" + "\n".join(overviews)

    def __str__(self):
        return self.get_overview()

    def get_submission_args(self, script, opts, depend=None):
        # self.session  opts["session"]
        # script = f"{opts['job']}"

        # opts = self._get_opts_("submit", opts)
        # if "queue" not in opts.keys():
        #     opts.update({"queue": None})

        # Finalize options
        opts.update(dict(script=script))
        if depend:
            opts["depend"] = ":".join(depend)
        if "extra " in opts:
            opts.update(opts.pop("extra"))

        # Format commandline arguments
        return self.get_command_args("submit", **opts)

    def submit(self, script, opts, depend=None):
        # Get submission arguments
        jobargs = self.get_submission_args(script, opts, depend=depend)

        # Submit
        logger.debug("Submit: " + " ".join(jobargs))
        res = subprocess.run(jobargs, capture_output=True, check=True)
        logger.debug("Submitted")

        # Parse result
        job = self._parse_submit_res_(res, jobargs)
        self.jobs.append(job)
        self.to_json(job)
        self.check_status(show=False)
        return job.jobid

    def check_status(self, show=True):
        self._get_jobs_session_()
        self.get_overview(jobids=[job.jobid for job in self.jobs])
        [self.to_json(job) for job in self.jobs]
        if show == True:
            print(self)

    def delete(self):
        self.check_status()
        cond = input(
            "Do you really want to delete the jobs listed hereabove ?(yes/no)"
        )
        if cond == "yes":
            args = self._get_command_args_(
                "delete",
                force="-W force",
                jobid=self._jobid_sep_().join(
                    [job.jobid for job in self.jobs]
                ),
            )
            print(args)
            subprocess.run(args, capture_output=True, check=True)

    def delete_force(self):
        self.check_status()
        args = self._get_command_args_(
            "delete",
            jobid=self._jobid_sep_().join([job.jobid for job in self.jobs]),
        )
        subprocess.run(args, capture_output=True, check=True)


class PbsproJobManager(BasicJobManager):
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

    def submit(self, opts):
        jobid = BasicJobManager.submit(self, opts)
        return jobid

    def _parse_submit_res_(self, res, jobargs):
        job = BasicJobManager._parse_submit_res_(
            self,
            res,
            jobargs,
        )
        job.jobid = res.stdout.decode("utf-8", errors="ignore").split(".")[0]
        return job

    def _jobid_sep_(self):
        return " "

    def _extra_status_args_(self, args):
        args.append("-x")
        args.append("-u $LOGNAME")
        return args

    def _parse_status_res_(self, res):
        """JOBID PARTITION NAME USER ST TIME NODES NODELIST(REASON)"""
        res = BasicJobManager._parse_status_res_(self, res)
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
                elaptime = datetime.timedelta(
                    seconds=int(ss), minutes=int(mm), hours=int(hh)
                )
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


class SlurmJobManager(BasicJobManager):
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

    def submit(self, opts):
        jobid = BasicJobManager.submit(self, opts)
        return jobid

    def _extra_status_args_(self, args):
        args.append("--noheader")
        return args

    def _jobid_sep_(self):
        return ","

    def _parse_submit_res_(self, res, jobargs):
        job = BasicJobManager._parse_submit_res_(self, res, jobargs)
        job.jobid = res.stdout.decode("utf-8", errors="ignore").split()[-1]
        return job

    def _parse_status_res_(self, res):
        """JOBID PARTITION NAME USER ST TIME NODES NODELIST(REASON)"""
        res = BasicJobManager._parse_status_res_(self, res)
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
