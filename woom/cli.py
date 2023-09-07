#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep  5 17:18:58 2023

@author: lsraynaud
"""

from argparse import ArgumentParser
from pathlib import Path
import logging

from . import hosts as whosts
from . import sessions as wsessions
from . import job as wjob
from . import tasks as wtasks
from . import workflow as wworkflow


def get_parser_run():
    # Setup argument parser
    parser = ArgumentParser(
        description=("woom executable"),
        # formatter_class=SloopArgumentHelpFormatter,
    )
    parser.add_argument(
        "--workflow-cfg",
        default="workflow.cfg",
        type=Path,
        help="workflow configuration file",
    )
    parser.add_argument(
        "--tasks-cfg",
        default="tasks.cfg",
        type=Path,
        help="tasks configuration file",
    )
    parser.add_argument(
        "--hosts-cfg", type=Path, help="hosts configuration file"
    )

    return parser


def main_run():
    # Get the parser
    parser = get_parser_run()

    # Parse args
    args = parser.parse_args()

    # Setup logging
    wjob.main_setup_logging(args)
    logger = logging.getLogger(__name__)

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
        logger.debug("Load session: " + args.session)
        session = session_manager.get_session(args.session)
        logger.info("Loaded session: " + session.id)
    else:
        logger.debug("Create new session")
        session = session_manager.create_session()
        logger.debug("Created new session: " + session.id)

    # Get task manager
    logger.debug("Initialize the task manager")
    taskmanager = wtasks.TaskManager(host)
    logger.info("Initialized the task manager")
    logger.debug("Load the task config file: " + args.tasks_cfg)
    taskmanager.load(args.tasks_cfg)
    logger.info("Loaded the task config file: " + args.tasks_cfg)

    # Get the workflow instance
    logger.debug("Initialize the workflow")
    worflow = wworkflow.Workflow(args.workflow_cfg, session, taskmanager)
    logger.debug("Initialized the workflow")
