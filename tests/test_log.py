#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for log.py module
"""
import argparse
import logging

import pytest

from woom import log as wlog


class TestLoggingConfig:
    """Test logging configuration"""

    def test_default_logging_config_structure(self):
        config = wlog.DEFAULT_LOGGING_CONFIG
        assert "version" in config
        assert config["version"] == 1
        assert "formatters" in config
        assert "handlers" in config
        assert "loggers" in config

    def test_formatters_in_config(self):
        config = wlog.DEFAULT_LOGGING_CONFIG
        assert "brief" in config["formatters"]
        assert "brief_no_color" in config["formatters"]
        assert "precise" in config["formatters"]

    def test_handlers_in_config(self):
        config = wlog.DEFAULT_LOGGING_CONFIG
        assert "console" in config["handlers"]
        assert "file" in config["handlers"]

    def test_loggers_in_config(self):
        config = wlog.DEFAULT_LOGGING_CONFIG
        assert "woom" in config["loggers"]


class TestSetupLogging:
    """Test setup_logging function"""

    def test_setup_logging_default(self):
        wlog.setup_logging(show_init_msg=False)
        logger = logging.getLogger("woom")
        assert logger is not None

    def test_setup_logging_console_level(self):
        wlog.setup_logging(console_level="DEBUG", show_init_msg=False)
        logger = logging.getLogger("woom")
        assert logger is not None

    def test_setup_logging_no_file(self):
        wlog.setup_logging(to_file=False, show_init_msg=False)
        logger = logging.getLogger("woom")
        assert logger is not None

    def test_setup_logging_custom_file(self, tmp_path):
        log_file = tmp_path / "custom.log"
        wlog.setup_logging(to_file=str(log_file), show_init_msg=False)
        logger = logging.getLogger("woom")
        assert logger is not None

    def test_setup_logging_no_color(self):
        wlog.setup_logging(no_color=True, show_init_msg=False)
        logger = logging.getLogger("woom")
        assert logger is not None


class TestParserArguments:
    """Test argument parser helpers"""

    def test_add_logging_parser_arguments(self):
        parser = argparse.ArgumentParser()
        wlog.add_logging_parser_arguments(parser)

        # Check that arguments are added
        args = parser.parse_args(["--log-level", "debug", "--log-no-color"])
        assert args.log_level == "debug"
        assert args.log_no_color is True

    def test_add_logging_parser_arguments_defaults(self):
        parser = argparse.ArgumentParser()
        wlog.add_logging_parser_arguments(parser, default_level="warning")

        args = parser.parse_args([])
        assert args.log_level == "warning"
        assert args.log_no_color is False

    def test_add_log_level_parser_arguments(self):
        parser = argparse.ArgumentParser()
        wlog.add_log_level_parser_arguments(parser)

        args = parser.parse_args(["--log-level", "error"])
        assert args.log_level == "error"

    def test_log_level_choices(self):
        parser = argparse.ArgumentParser()
        wlog.add_log_level_parser_arguments(parser)

        # Valid choices
        for level in ["debug", "info", "warning", "error", "critical"]:
            args = parser.parse_args(["--log-level", level])
            assert args.log_level == level

        # Invalid choice should raise error
        with pytest.raises(SystemExit):
            parser.parse_args(["--log-level", "invalid"])


class TestMainSetupLogging:
    """Test main_setup_logging function"""

    def test_main_setup_logging(self, tmp_path):
        parser = argparse.ArgumentParser()
        wlog.add_logging_parser_arguments(parser)
        args = parser.parse_args(["--log-level", "info"])

        log_file = tmp_path / "test.log"
        wlog.main_setup_logging(args, to_file=str(log_file))

        logger = logging.getLogger("woom")
        assert logger is not None

    def test_main_setup_logging_no_color(self):
        parser = argparse.ArgumentParser()
        wlog.add_logging_parser_arguments(parser)
        args = parser.parse_args(["--log-level", "debug", "--log-no-color"])

        wlog.main_setup_logging(args, to_file=False)

        logger = logging.getLogger("woom")
        assert logger is not None


class TestLoggingLevels:
    """Test that different logging levels work"""

    def test_debug_level(self, tmp_path, caplog):
        log_file = tmp_path / "debug.log"
        wlog.setup_logging(console_level="DEBUG", to_file=str(log_file), show_init_msg=False)

        logger = logging.getLogger("woom.test")
        with caplog.at_level(logging.DEBUG):
            logger.debug("Debug message")
            assert "Debug message" in caplog.text

    def test_info_level(self, tmp_path, caplog):
        log_file = tmp_path / "info.log"
        wlog.setup_logging(console_level="INFO", to_file=str(log_file), show_init_msg=False)

        logger = logging.getLogger("woom.test")
        with caplog.at_level(logging.INFO):
            logger.info("Info message")
            assert "Info message" in caplog.text

    def test_warning_level(self, tmp_path, caplog):
        log_file = tmp_path / "warning.log"
        wlog.setup_logging(console_level="WARNING", to_file=str(log_file), show_init_msg=False)

        logger = logging.getLogger("woom.test")
        with caplog.at_level(logging.WARNING):
            logger.warning("Warning message")
            assert "Warning message" in caplog.text
