#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Logging utilities
"""

import logging.config

DEFAULT_LOGGING_CONFIG = {
    "version": 1,
    #    'incremental': True,
    "disable_existing_loggers": False,
    "formatters": {
        "brief": {
            "format": (
                "%(log_color)s%(name)-12s: %(bold)s%(levelname)-8s"
                "%(bold)s%(reset)s%(log_color)s %(message)s%(reset)s"
            ),
            "()": "colorlog.ColoredFormatter",
            "log_colors": {
                "DEBUG": "thin",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        },
        "brief_no_color": {
            "format": "%(name)-12s: %(levelname)-8s %(message)s",
        },
        "precise": {
            "format": "%(asctime)s %(name)-12s: %(levelname)-8s %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "brief",
            "level": "INFO",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "precise",
            "filename": "woom.log",
            "maxBytes": 1024 * 1000,
            "backupCount": 3,
            "level": "DEBUG",
        },
    },
    "loggers": {"woom": {"handlers": ["console", "file"], "level": "DEBUG"}},
}


def setup_logging(console_level=None, to_file=True, no_color=False, show_init_msg=True, **kwargs):
    """Setup the logging"""

    #    for handler in logging.root.handlers:
    #        logging.root.handlers.remove(handler)
    #    del logging.root.handlers[:]

    # Alter the config
    logging_config = DEFAULT_LOGGING_CONFIG.copy()
    if console_level is not None:
        logging_config["handlers"]["console"]["level"] = console_level.upper()
    if to_file is False and "file" in logging_config["loggers"]["woom"]["handlers"]:
        logging_config["loggers"]["woom"]["handlers"].remove("file")
    else:
        fconfig = logging_config["handlers"]["file"]
        if isinstance(to_file, str):
            fconfig["filename"] = to_file
    if no_color:
        logging_config["handlers"]["console"]["formatter"] = "brief_no_color"

    # Update config
    logging_config.update(kwargs)

    # Load it
    logging.config.dictConfig(logging_config)
    if show_init_msg:
        logging.getLogger(__name__).debug("*** STARTED LOG SESSION ***")


def add_logging_parser_arguments(parser, default_level="info"):
    add_log_level_parser_arguments(parser, default_level=default_level)
    # parser.add_argument("--log-file", help="logging file name", default=default_log_file)
    parser.add_argument(
        "--log-no-color",
        help="suppress colors in console",
        action="store_true",
    )


def add_log_level_parser_arguments(parser, default_level="info"):
    parser.add_argument(
        "--log-level",
        help="logging level",
        choices=["debug", "info", "warning", "error", "critical"],
        default=default_level,
    )


def main_setup_logging(args, to_file=True):
    setup_logging(
        console_level=args.log_level,
        to_file=to_file,
        no_color=args.log_no_color,
    )
