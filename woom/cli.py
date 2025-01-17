#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Commandline interface
"""

import os
import argparse

# from pathlib import Path
import logging
import shutil
import pandas as pd

from . import util as wutil
from . import hosts as whosts
from . import sessions as wsessions

# from . import job as wjob
from . import tasks as wtasks
from . import workflow as wworkflow
from . import conf as wconf
from . import log as wlog


def get_parser():
    parser = argparse.ArgumentParser(
        description="woom interface",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(help="sub-command help")

    add_parser_run(subparsers)
    # add_parser_sessions(subparsers)

    return parser


def main():
    # Get the parser
    parser = get_parser()

    # Parse args
    args = parser.parse_args()

    # Call subparser function
    if hasattr(args, "func"):
        args.func(parser, args)
    elif hasattr(args, "subcommands"):
        parser.exit(0, "please use one of the subcommands: " f"{args.subcommands}\n")
    else:
        parser.print_usage()


def add_app_arguments(parser):
    parser.add_argument(
        "--app-name",
        help="application name",
    )
    parser.add_argument(
        "--app-conf",
        help="application configuration",
    )
    parser.add_argument(
        "--app-exp",
        help="application experiment",
    )


def setup_workflow(parser, args, clean):
    # Workflow dir from workflow config file
    workflow_cfg = os.path.abspath(args.workflow_cfg)
    if not os.path.exists(workflow_cfg):
        parser.error(f"Workflow configuration file not found: {args.workflow_cfg}")
    workflow_dir = os.path.dirname(workflow_cfg)

    # Clean
    if clean:
        for subdir in wworkflow.Workflow.output_directories:
            path = os.path.join(workflow_dir, subdir)
            if os.path.exists(path):
                shutil.rmtree(path, ignore_errors=True)

    # Setup logging
    log_file = wutil.check_dir(os.path.join(workflow_dir, "log", "woom.log"), logger=False)
    wlog.main_setup_logging(args, to_file=log_file)
    logger = logging.getLogger(__name__)

    # Load workflow config
    logger.debug(f"Load workflow config: {workflow_cfg}")
    workflow_config = wconf.load_cfg(workflow_cfg, wworkflow.CFGSPECS_FILE)
    logger.info("Loaded workflow config")

    # App
    wconf.merge_args_with_config(workflow_config, args, ["name", "conf", "exp"], prefix="app_")
    app_name = workflow_config["app"]["name"]
    app_conf = workflow_config["app"]["conf"]
    app_exp = workflow_config["app"]["exp"]
    if app_name:
        logger.info(f"App name: {app_name}")
    if app_conf:
        logger.info(f"App conf: {app_conf}")
    if app_exp:
        logger.info(f"App exp: {app_exp}")
    app = dict(app_name=app_name, app_conf=app_conf, app_exp=app_exp)

    # Cycles
    wconf.merge_args_with_config(
        workflow_config["cycles"],
        args,
        ["begin_date", "end_date", "freq", "ncycle"],
    )

    # Get host
    logger.debug("Initialize the host manager")
    hostmanager = whosts.HostManager()
    logger.info("Initialized the host manager")
    if args.hosts_cfg:
        logger.debug("Load hosts config file: " + args.hosts_cfg)
        hostmanager.load_config(args.hosts_cfg)
        logger.info("Loaded hosts config file: " + args.hosts_cfg)
    if args.host:
        logger.debug("Get host instance: " + args.host)
        host = hostmanager.get_host(args.host)
        logger.info("Got host instance: " + args.host)
    else:
        logger.debug("Infer host")
        host = hostmanager.infer_host()
        logger.info("Infered host: " + host.name)

    ## Get session
    # logger.debug("Initialize the session manager")
    # session_manager = wsessions.SessionManager()
    # logger.info("Initialized the session manager")
    # if args.session:
    # if args.session.lower() == "latest":
    # logger.debug("Finding latest session")
    # args.session = session_manager.get_latest(**app)
    # if args.session:
    # logger.info(f"Found latest session: {args.session}")
    # if args.session not in session_manager:
    # logger.debug("Create explicit new session: " + args.session)
    # session = session_manager.create_session(args.session)
    # logger.info("Created new session: " + session.id)
    # session.update(app)
    # else:
    # logger.debug("Load session: " + args.session)
    # session = session_manager.get_session(args.session)
    # if not session.check_matching_items(**app):
    # raise Exception("Loaded session is incompatible with app")
    # logger.info(f"Loaded session: {session}")
    # if args.clean:
    # session.clean()
    # else:
    # logger.debug("Create new session")
    # session = session_manager.create_session()
    # logger.info("Created new session: " + session.id)
    # session.update(app)

    # Init task manager
    logger.debug("Initialize the task manager")
    taskmanager = wtasks.TaskManager(host)  # , session)
    logger.info("Initialized the task manager")
    logger.debug("Load the task config file: " + args.tasks_cfg)
    taskmanager.load_config(args.tasks_cfg)
    logger.info("Loaded the task config file: " + args.tasks_cfg)

    # Init workflow
    logger.debug("Initialize the workflow")
    workflow = wworkflow.Workflow(workflow_config, taskmanager)
    logger.info("Initialized the workflow")

    return workflow, logger


def add_parser_run(subparsers):
    # Setup argument parser
    parser_run = subparsers.add_parser("run", help="run a workflow")
    parser_run.add_argument(
        "--dry-run",
        help="run in fake mode for testing purpose",
        action="store_true",
    )
    parser_run.add_argument(
        "--clean",
        help="remove session directory and output directory first, like log/ and tasks/",
        action="store_true",
    )
    add_app_arguments(parser_run)
    parser_run.add_argument(
        "--workflow-cfg",
        default="workflow.cfg",
        help="workflow configuration file",
    )
    parser_run.add_argument(
        "--tasks-cfg",
        default="tasks.cfg",
        help="tasks configuration file",
    )
    parser_run.add_argument("--hosts-cfg", help="hosts configuration file", default="hosts.cfg")
    parser_run.add_argument("--host", help="target host")
    parser_run.add_argument("--session", help="target session")
    parser_run.add_argument("--begin-date", help="begin date", type=wconf.is_datetime)
    parser_run.add_argument("--end-date", help="end date", type=wconf.is_datetime)
    parser_run.add_argument("--freq", help="interval between cycles", type=wconf.is_timedelta)
    parser_run.add_argument("--ncycle", help="number of cycles", type=int)
    wlog.add_logging_parser_arguments(parser_run)
    parser_run.set_defaults(func=main_run)

    return parser_run


def main_run(parser, args):
    # Setup the workflow
    workflow, logger = setup_workflow(parser, args, args.clean)

    # Run the workflow
    logger.debug("Run the workflow")
    workflow.run(dry=args.dry_run)
    logger.info("Successfully ran the workflow!")


def add_parser_status(subparsers):
    # Setup argument parser
    parser_status = subparsers.add_parser("status", help="run a workflow")
    parser_status.add_argument(
        "--dry-run",
        help="run in fake mode for testing purpose",
        action="store_true",
    )
    add_app_arguments(parser_status)
    parser_status.add_argument(
        "--workflow-cfg",
        default="workflow.cfg",
        help="workflow configuration file",
    )
    parser_status.add_argument(
        "--tasks-cfg",
        default="tasks.cfg",
        help="tasks configuration file",
    )
    parser_status.add_argument("--hosts-cfg", help="hosts configuration file", default="hosts.cfg")
    parser_status.add_argument("--host", help="target host")
    parser_status.add_argument("--begin-date", help="begin date", type=wconf.is_datetime)
    parser_status.add_argument("--end-date", help="end date", type=wconf.is_datetime)
    parser_status.add_argument("--freq", help="interval between cycles", type=wconf.is_timedelta)
    parser_status.add_argument("--ncycle", help="number of cycles", type=int)
    wlog.add_logging_parser_arguments(parser_status)
    parser_status.set_defaults(func=main_run)

    return parser_status


# def add_parser_sessions(subparsers):
## Setup argument parser
# parser_sessions = subparsers.add_parser("sessions", help="manage sessions")
# add_app_arguments(parser_sessions)
# wlog.add_logging_parser_arguments(parser_sessions)

# subparsers_sessions = parser_sessions.add_subparsers(help="sessions sub-command help")

## list
# parser_list = subparsers_sessions.add_parser("list", help="list sessions")
# parser_list.set_defaults(func=main_sessions_list)

## remove
# parser_remove = subparsers_sessions.add_parser("remove", help="remove sessions")
# parser_remove.add_argument("session_id", help="selected session ids", nargs="*")
# parser_remove.add_argument("--max-age", help="Max allowed age", type=pd.to_timedelta)
# parser_remove.set_defaults(func=main_sessions_remove)


# def main_sessions_list(parser, args):
# wlog.main_setup_logging(args, to_file=False)
# session_manager = wsessions.SessionManager()
# sessions = session_manager.get_matching_sessions(
# app_name=args.app_name, app_conf=args.app_conf, app_exp=args.app_exp
# )
# session_manager.nice_print(sessions)


# def main_sessions_remove(parser, args):
# wlog.main_setup_logging(args, to_file=False)
# session_manager = wsessions.SessionManager()
# session_manager.remove(
# args.session_id or None,
# max_age=args.max_age,
# app_name=args.app_name,
# app_conf=args.app_conf,
# app_exp=args.app_exp,
# )
