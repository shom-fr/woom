#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Commandline interface
"""

import argparse

# from pathlib import Path
import logging
import os

# from . import job as wjob
from . import conf as wconf
from . import ext as wext
from . import hosts as whosts
from . import log as wlog
from . import tasks as wtasks
from . import util as wutil
from . import workflow as wworkflow

# %% Main


def get_parser():
    parser = argparse.ArgumentParser(
        description="woom interface",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--app-name", help="application name")
    parser.add_argument("--app-conf", help="application configuration")
    parser.add_argument("--app-exp", help="application experiment")
    parser.add_argument("--workflow-cfg", default="workflow.cfg", help="workflow configuration file")
    parser.add_argument(
        "--workflow-ini",
        default="workflow.ini",
        help="user workflow configuration specifications file",
    )
    parser.add_argument("--tasks-cfg", default="tasks.cfg", help="tasks configuration file")
    parser.add_argument("--hosts-cfg", help="hosts configuration file", default="hosts.cfg")
    parser.add_argument("--host", help="target host as described in the hosts configuration file")
    parser.add_argument("--begin-date", help="begin date", type=wconf.is_datetime)
    parser.add_argument("--end-date", help="end date", type=wconf.is_datetime)
    parser.add_argument("--freq", help="interval between cycles")
    parser.add_argument("--ncycle", help="number of cycles", type=int)

    subparsers = parser.add_subparsers(help="sub-command help")

    add_parser_show(subparsers)
    add_parser_run(subparsers)
    add_parser_kill(subparsers)
    add_parser_clean(subparsers)

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


def get_workflow_cfg(parser, args):
    """Workflow dir from workflow config file"""
    workflow_cfg = os.path.abspath(args.workflow_cfg)
    if not os.path.exists(workflow_cfg):
        parser.error(f"Workflow configuration file not found: {args.workflow_cfg}")
    return workflow_cfg


def setup_logger(workflow_dir, args):
    log_file = wutil.check_dir(os.path.join(workflow_dir, "log", "woom.log"), logger=False)
    wlog.main_setup_logging(args, to_file=log_file)
    return logging.getLogger(__name__)


def setup_workflow(parser, args):
    # Workflow file
    workflow_cfg = get_workflow_cfg(parser, args)

    # Get logger
    logger = setup_logger(os.path.dirname(workflow_cfg), args)

    # Setup the workflow
    logger.debug("Run the workflow")
    try:
        workflow = get_workflow(workflow_cfg, logger, parser, args)
    except Exception as e:
        logger.exception(f"Workflow setup failed: {e.args[0]}")
        return None, None
    else:
        logger.info("Successfully setup the workflow!")
        return workflow, logger


def get_workflow(workflow_cfg, logger, parser, args):  # , clean):
    # # Workflow dir
    workflow_dir = os.path.dirname(workflow_cfg)

    # Load extensions
    logger.debug("Loading extensions")
    exts = wext.load_extensions(workflow_dir)
    if exts:
        logger.info("Loaded extensions: " + ", ".join(exts))
    else:
        logger.info("No extension to load")

    # Load workflow config
    workflow_cfgspecs = [wworkflow.CFGSPECS_FILE]
    if os.path.exists(args.workflow_ini):
        logger.info(
            f"Using user specific file for workflow configuration specifications: {args.workflow_ini}"
        )
        workflow_cfgspecs.append(args.workflow_ini)
    logger.debug(f"Load workflow config: {workflow_cfg}")
    workflow_config = wconf.load_cfg(workflow_cfg, workflow_cfgspecs, list_values=True)
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
    # app = dict(app_name=app_name, app_conf=app_conf, app_exp=app_exp)

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

    # Init task manager
    logger.debug("Initialize the task manager")
    taskmanager = wtasks.TaskManager(host)
    logger.info("Initialized the task manager")
    logger.debug("Load the task config file: " + args.tasks_cfg)
    taskmanager.load_config(args.tasks_cfg)
    logger.info("Loaded the task config file: " + args.tasks_cfg)

    # Init workflow
    logger.debug("Initialize the workflow")
    workflow = wworkflow.Workflow(workflow_config, taskmanager)
    logger.info("Initialized the workflow")

    return workflow


# %% Show


def add_parser_show(subparsers):
    # Setup argument parser
    parser_show = subparsers.add_parser(
        "show",
        help="show info about the workflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    subparsers_show = parser_show.add_subparsers(help="sub-command help")
    add_parser_show_overview(subparsers_show)
    add_parser_show_status(subparsers_show)
    add_parser_show_run_dirs(subparsers_show)
    add_parser_show_artifacts(subparsers_show)

    return parser_show


def add_parser_show_overview(subparsers):
    # Setup argument parser
    parser_show_overview = subparsers.add_parser(
        "overview",
        help="show main info like the task tree and cycles",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    wlog.add_logging_parser_arguments(parser_show_overview, default_level="warning")
    parser_show_overview.set_defaults(func=main_show_overview)

    return parser_show_overview


def main_show_overview(parser, args):
    # Setup the workflow
    workflow, logger = setup_workflow(parser, args)
    if not workflow:
        return

    # Show the status
    try:
        workflow.show_overview()
    except Exception:
        logger.exception("Failed to display the overview")


def add_parser_show_status(subparsers):
    # Setup argument parser
    parser_show_status = subparsers.add_parser(
        "status",
        help="get the status of all jobs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_show_status.add_argument("-r", "--running", help="show only running jobs", action="store_true")
    parser_show_status.add_argument(
        "--tablefmt", help="table format (see the tabulate package)", default="rounded_outline"
    )
    parser_show_status.add_argument("--no-color", help="don't colorize the status", action="store_true")
    wlog.add_logging_parser_arguments(parser_show_status, default_level="warning")
    parser_show_status.set_defaults(func=main_show_status)

    return parser_show_status


def main_show_status(parser, args):
    # Setup the workflow
    workflow, logger = setup_workflow(parser, args)
    if not workflow:
        return

    # Show the status
    try:
        workflow.show_status(tablefmt=args.tablefmt, running=args.running, colorize=not args.no_color)
    except Exception:
        logger.exception("Failed querying the status")


def add_parser_show_run_dirs(subparsers):
    # Setup argument parser
    parser_show_run_dirs = subparsers.add_parser(
        "run_dirs",
        help="show the run directory of all worklow tasks",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_show_run_dirs.add_argument(
        "--tablefmt", help="table format (see the tabulate package)", default="rounded_outline"
    )
    wlog.add_logging_parser_arguments(parser_show_run_dirs, default_level="warning")
    parser_show_run_dirs.set_defaults(func=main_show_run_dirs)

    return parser_show_run_dirs


def main_show_run_dirs(parser, args):
    # Setup the workflow
    workflow, logger = setup_workflow(parser, args)
    if not workflow:
        return

    # Show
    try:
        workflow.show_run_dirs(tablefmt=args.tablefmt)
    except Exception:
        logger.exception("Failed showing the run directories")


def add_parser_show_artifacts(subparsers):
    # Setup argument parser
    parser_show_artifacts = subparsers.add_parser(
        "artifacts",
        help="show the run directory of all worklow tasks",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_show_artifacts.add_argument(
        "--tablefmt", help="table format (see the tabulate package)", default="rounded_outline"
    )
    wlog.add_logging_parser_arguments(parser_show_artifacts, default_level="warning")
    parser_show_artifacts.set_defaults(func=main_show_artifacts)

    return parser_show_artifacts


def main_show_artifacts(parser, args):
    # Setup the workflow
    workflow, logger = setup_workflow(parser, args)
    if not workflow:
        return

    # Show
    try:
        workflow.show_artifacts(tablefmt=args.tablefmt)
    except Exception:
        logger.exception("Failed showing the run directories")


# %% Run


def add_parser_run(subparsers):
    # Setup argument parser
    parser_run = subparsers.add_parser(
        "run",
        help="run a workflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_run.add_argument(
        "--dry-run",
        "--test",
        help="run in fake mode for testing purpose",
        action="store_true",
    )
    parser_run.add_argument(
        "--update",
        help="do not run if it has already been run",
        action="store_true",
    )
    wlog.add_logging_parser_arguments(parser_run)
    parser_run.set_defaults(func=main_run)

    return parser_run


def main_run(parser, args):
    # Setup the workflow
    workflow, logger = setup_workflow(parser, args)
    if not workflow:
        return

    # Run the workflow
    logger.debug("Run the workflow")
    try:
        workflow.run(dry=args.dry_run, update=args.update)
    except Exception as e:
        logger.exception(f"Workflow failed: {e.args[0]}")
    else:
        logger.info("Successfully ran the workflow!")


# %% Kill


def add_parser_kill(subparsers):
    # Setup argument parser
    parser_kill = subparsers.add_parser(
        "kill",
        help="kill one or all workflow jobs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_kill.add_argument("jobid", help="job id", nargs="*")
    parser_kill.add_argument("--task", help="kill this task only", default=None)
    parser_kill.add_argument("--cycle", help="kill this cycle only", default=None)
    parser_kill.add_argument(
        "--dry-run",
        help="run in fake mode for testing purpose",
        action="store_true",
    )
    wlog.add_logging_parser_arguments(parser_kill, default_level="warning")
    parser_kill.set_defaults(func=main_kill)

    return parser_kill


def main_kill(parser, args):
    # Setup the workflow
    workflow, logger = setup_workflow(parser, args)
    if not workflow:
        return

    # Kill
    try:
        workflow.kill(jobid=args.jobid, task_name=args.task, cycle=args.cycle)
    except Exception:
        logger.exception("Failed to kill jobs")


# %% Clean


def add_parser_clean(subparsers):
    # Setup argument parser
    parser_clean = subparsers.add_parser(
        "clean",
        help="remove temporary files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_clean.add_argument("extra_file", help="extra file or directory to remove", nargs="*")
    parser_clean.add_argument(
        "--without-submission-dirs",
        help="do not remove submission directories",
        action="store_true",
    )
    parser_clean.add_argument("--with-run-dirs", help="remove run directories", action="store_true")
    parser_clean.add_argument("--with-log-files", help="remove log files", action="store_true")
    parser_clean.add_argument("--with-artifacts", help="remove artifacts", action="store_true")
    parser_clean.add_argument(
        "--dry-run",
        "--test",
        help="run in fake mode for testing purpose",
        action="store_true",
    )
    wlog.add_logging_parser_arguments(parser_clean, default_level="info")
    parser_clean.set_defaults(func=main_clean)

    return parser_clean


def main_clean(parser, args):
    # Setup the workflow
    workflow, logger = setup_workflow(parser, args)
    if not workflow:
        return

    # Kill running jobs
    if not args.dry_run:
        try:
            workflow.kill()
        except Exception:
            logger.exception("Failed to kill all jobs")

    # Show the status
    try:
        workflow.clean(
            submission_dirs=not args.without_submission_dirs,
            run_dirs=args.with_run_dirs,
            log_files=args.with_log_files,
            artifacts=args.with_artifacts,
            dry=args.dry_run,
        )
    except Exception:
        logger.exception("Failed to clean workflow")
