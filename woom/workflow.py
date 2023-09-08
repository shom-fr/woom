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

from . import conf as wconf

CFGSPECS_FILE = os.path.join(os.path.basename(__file__), "workflow.ini")


def _get_scalar_items_(cfg):
    return {(k, cfg["k"]) for k in cfg.scalars}


class Workflow:
    def __init__(self, cfgfile, session, taskmanager):
        if isinstance(cfgfile, str):
            self._cfgfile = cfgfile
            self._config = wconf.load_cfg(cfgfile, CFGSPECS_FILE)
        else:
            self._config = cfgfile
            self._cfgfile = self._config.filename
        self._tm = taskmanager
        self._session = session
        if self._config["app"]["name"]:
            session["app"] = self._config["app"]["name"]
        if self._config["app"]["conf"]:
            session["conf"] = self._config["app"]["conf"]
        if self._config["app"]["exp"]:
            session["exp"] = self._config["app"]["exp"]

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
        self.host.get_jobmanager(self.session)

    def _populate_params_(self, taskname, **extra):
        """Fill the params dictionnary

        Order  with the last crushing the first:

        - [params] scalars
        - Host generic directories "[dirs] work" -> "workdir"
        - Task [[<task>]] scalars
        - Host-task [[<task>]]/[[[<host>]]] scalars
        - Extra
        """
        # Generic params
        params = _get_scalar_items_(self._config["params"])

        # Get host generic dirs
        for key, val in self.host["dirs"]:
            if val:
                params[key + "dir"] = val

        # Get task specific params
        if taskname in self._config["params"]:
            params.update(_get_scalar_items_(self._config["params"][taskname]))

            # Host specific params for this task
            if self.host.name in params["taskname"]:
                params.update(
                    _get_scalar_items_(
                        self._config["params"][taskname][self.host.name]
                    )
                )

        # Extra parameters
        if extra:
            params.update(extra)

        return params

    def _run_task_(self, name, params):
        # Get task bash code and submission options
        task_specs = self.taskmanager.export_task(name, params)

        # Create the bash submission script in cache
        token = secrets.token_hex(8)
        date = datetime.datetime.utcnow()
        fname = f"batch-{name}-{date:%Y-%m-%d-%H-%M-%S}-{token}.sh"
        with self.session.open_file("batch", fname, "w") as f:
            f.write(task_specs["script_content"])

        # Submit it
        args = task_specs["scheduler_options"]
        args["job"] = self.session.get_file_name("batch", fname)
        args["session"] = str(self.session)
        job = self.jobmanager.submit(args)

        return job

    def run(self, startdate=None, enddate=None, freq=None, ncycle=None):
        pass
