#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep  5 17:18:58 2023

@author: lsraynaud
"""

import argparse
from pathlib import Path
import logging

from . import hosts as whosts
from . import sessions as wsessions
from . import job as wjob
from . import tasks as wtasks
from . import workflow as wworkflow
from . import conf as wconf


def get_parser():
    parser = argparse.ArgumentParser(
        description="woom interface",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(help="sub-command help")

    add_parser_run(subparsers)


def main():
    parser = get_parser()
    args = parser.parse_args()
    args.func(parser, args)


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


def add_parser_run(subparsers):
    # Setup argument parser
    parser_run = subparsers.add_parser("run", help="run a workflow")
    add_app_arguments(parser_run)
    parser_run.add_argument(
        "--workflow-cfg",
        default="workflow.cfg",
        type=Path,
        help="workflow configuration file",
    )
    parser_run.add_argument(
        "--tasks-cfg",
        default="tasks.cfg",
        type=Path,
        help="tasks configuration file",
    )
    parser_run.add_argument(
        "--hosts-cfg", type=Path, help="hosts configuration file"
    )
    parser_run.set_defaults(func=main_run)

    return parser_run


def main_run(parser, args):
    # Setup logging
    wjob.main_setup_logging(args)
    logger = logging.getLogger(__name__)

    # Load workflow config
    logger.debug("Load workflow config")
    workflow_config = wconf.load_cfg(
        args.workflow_cfg, wworkflow.CFGSPECS_FILE
    )
    logger.info("Loaded workflow config")
    if args.app_name:
        workflow_config["app"]["name"] = args.app_name
    if args.app_conf:
        workflow_config["app"]["conf"] = args.app_conf
    if args.app_exp:
        workflow_config["app"]["exp"] = args.app_exp
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
        logger.debug("Infered host: " + host.name)

    # Get session
    logger.debug("Initialize the session manager")
    session_manager = wsessions.SessionManager()
    logger.info("Initialized the session manager")
    if args.session:
        if args.session.lower() == "latest":
            logger.debug("Load latest session")
            session = session_manager.get_latest_session(**app)
            logger.info(f"Loaded lession: {session}")
        else:
            logger.debug("Load session: " + args.session)
            session = session_manager.get_session(args.session)
            if not session.check_matching_items(session, **app):
                raise Exception("Loaded session is incompatible with app")
            logger.info(f"Loaded session: {session}")
    else:
        logger.debug("Create new session")
        session = session_manager.create_session()
        logger.debug("Created new session: " + session.id)
        session.update(app)

    # Get task manager
    logger.debug("Initialize the task manager")
    taskmanager = wtasks.TaskManager(host)
    logger.info("Initialized the task manager")
    logger.debug("Load the task config file: " + args.tasks_cfg)
    taskmanager.load(args.tasks_cfg)
    logger.info("Loaded the task config file: " + args.tasks_cfg)

    # Get the workflow instance
    logger.debug("Initialize the workflow")
    worflow = wworkflow.Workflow(workflow_config, session, taskmanager)
    logger.debug("Initialized the workflow")


def add_parser_sessions(subparsers):
    # Setup argument parser
    parser_sessions = subparsers.add_parser("sessions", help="manage sessions")
    add_app_arguments(parser_sessions)

    subparsers_sessions = parser_sessions.add_subparsers(
        help="sessions sub-command help"
    )

    parser_list = subparsers_sessions.add_parser("list", help="list sessions")
    parser_list.set_defaults(func=main_sessions_list)


def main_sessions_list(parser, args):
    session_manager = wsessions.SessionManager()
    sessions = session_manager.get_matching_sessions(
        app_name=args.app_name, app_conf=args.app_conf, app_exp=args.app_exp
    )
    session_manager.nice_print(sessions)
