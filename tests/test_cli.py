#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for woom.cli module

Run: pytest test_cli.py -v
"""
import argparse
import os
from unittest.mock import MagicMock, Mock, mock_open, patch

import pytest

from woom.cli import add_parser_fill, get_parser, main_fill


class TestParserFill:
    """Test add_parser_fill function"""

    def test_add_parser_fill_creates_subparser(self):
        """Test that add_parser_fill creates a fill subparser"""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()

        fill_parser = add_parser_fill(subparsers)

        assert fill_parser is not None
        assert fill_parser.prog.endswith('fill')

    def test_add_parser_fill_has_required_arguments(self):
        """Test that fill parser has required template and destination arguments"""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()

        fill_parser = add_parser_fill(subparsers)

        # Parse test arguments
        args = fill_parser.parse_args(['template.j2', 'output.txt'])

        assert args.template == 'template.j2'
        assert args.destination == 'output.txt'

    def test_add_parser_fill_has_optional_arguments(self):
        """Test that fill parser has optional task-name, cycle, member arguments"""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()

        fill_parser = add_parser_fill(subparsers)

        # Parse test arguments with optional flags
        args = fill_parser.parse_args(
            ['template.j2', 'output.txt', '--task-name', 'mytask', '--cycle', '2020-01-01', '--member', '1']
        )

        assert args.task_name == 'mytask'
        assert args.cycle == '2020-01-01'
        assert args.member == '1'

    def test_add_parser_fill_has_dry_run_flag(self):
        """Test that fill parser has dry-run flag"""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()

        fill_parser = add_parser_fill(subparsers)

        # Parse test arguments with dry-run
        args = fill_parser.parse_args(['template.j2', 'output.txt', '--dry-run'])

        assert args.dry_run is True

    def test_add_parser_fill_environment_variables(self):
        """Test that parser picks up environment variables for defaults"""
        # Set environment variables
        with patch.dict(
            os.environ, {'WOOM_TASK_NAME': 'env_task', 'WOOM_CYCLE': 'env_cycle', 'WOOM_MEMBER': 'env_member'}
        ):
            parser = argparse.ArgumentParser()
            subparsers = parser.add_subparsers()
            fill_parser = add_parser_fill(subparsers)

            # Parse without providing optional arguments
            args = fill_parser.parse_args(['template.j2', 'output.txt'])

            assert args.task_name == 'env_task'
            assert args.cycle == 'env_cycle'
            assert args.member == 'env_member'


class TestMainFill:
    """Test main_fill function"""

    @patch('woom.cli.setup_workflow')
    @patch('woom.cli.wrender.JINJA_ENV.get_template')
    @patch('woom.cli.wrender.render')
    @patch('woom.cli.wutil.check_dir')
    @patch('builtins.open', new_callable=mock_open)
    def test_main_fill_basic(self, mock_file, mock_check_dir, mock_render, mock_get_template, mock_setup):
        """Test basic fill operation"""
        # Setup mocks
        mock_workflow = Mock()
        mock_logger = Mock()
        mock_setup.return_value = (mock_workflow, mock_logger)

        mock_template = MagicMock()
        mock_get_template.return_value = mock_template
        mock_render.return_value = 'rendered content'
        mock_check_dir.return_value = '/tmp/output.txt'

        # Create argument namespace
        parser = Mock()
        args = argparse.Namespace(
            template='template.j2',
            destination='/tmp/output.txt',
            task_name=None,
            cycle=None,
            member=None,
            dry_run=False,
        )

        # Execute
        result = main_fill(parser, args)

        # Verify
        assert result == 0
        mock_get_template.assert_called_once_with('template.j2')
        mock_render.assert_called_once()
        mock_file.assert_called_once_with('/tmp/output.txt', 'w')
        mock_file().__enter__().write.assert_called_once_with('rendered content')

    @patch('woom.cli.setup_workflow')
    @patch('woom.cli.wrender.JINJA_ENV.get_template')
    @patch('woom.cli.wrender.render')
    @patch('woom.cli.wutil.check_dir')
    @patch('builtins.open', new_callable=mock_open)
    def test_main_fill_with_task_context(
        self, mock_file, mock_check_dir, mock_render, mock_get_template, mock_setup
    ):
        """Test fill with task context"""
        # Setup mocks
        mock_workflow = Mock()
        mock_logger = Mock()
        mock_setup.return_value = (mock_workflow, mock_logger)

        mock_template = MagicMock()
        mock_get_template.return_value = mock_template
        mock_render.return_value = 'rendered content with context'
        mock_check_dir.return_value = '/tmp/output.txt'

        # Create argument namespace
        parser = Mock()
        args = argparse.Namespace(
            template='template.j2',
            destination='/tmp/output.txt',
            task_name='mytask',
            cycle='2020-01-01',
            member='1',
            dry_run=False,
        )

        # Execute
        result = main_fill(parser, args)

        # Verify
        assert result == 0
        mock_workflow.set_context.assert_called_once_with(task_name='mytask', cycle='2020-01-01', member='1')
        mock_file().__enter__().write.assert_called_once_with('rendered content with context')

    @patch('woom.cli.setup_workflow')
    @patch('woom.cli.wrender.JINJA_ENV.get_template')
    @patch('woom.cli.wrender.render')
    @patch('woom.cli.wutil.check_dir')
    @patch('builtins.open', new_callable=mock_open)
    def test_main_fill_dry_run(self, mock_file, mock_check_dir, mock_render, mock_get_template, mock_setup):
        """Test fill in dry-run mode"""
        # Setup mocks
        mock_workflow = Mock()
        mock_logger = Mock()
        mock_setup.return_value = (mock_workflow, mock_logger)

        mock_template = MagicMock()
        mock_get_template.return_value = mock_template
        mock_render.return_value = 'rendered content'
        mock_check_dir.return_value = '/tmp/output.txt'

        # Create argument namespace
        parser = Mock()
        args = argparse.Namespace(
            template='template.j2',
            destination='/tmp/output.txt',
            task_name=None,
            cycle=None,
            member=None,
            dry_run=True,
        )

        # Execute
        result = main_fill(parser, args)

        # Verify
        assert result == 0
        mock_get_template.assert_called_once_with('template.j2')
        mock_render.assert_called_once()
        # In dry run mode, file should NOT be written
        mock_file.assert_not_called()

    @patch('woom.cli.setup_workflow')
    def test_main_fill_no_workflow(self, mock_setup):
        """Test fill when workflow setup fails"""
        # Setup mocks
        mock_setup.return_value = (None, None)

        # Create argument namespace
        parser = Mock()
        args = argparse.Namespace(
            template='template.j2',
            destination='/tmp/output.txt',
            task_name=None,
            cycle=None,
            member=None,
            dry_run=False,
        )

        # Execute
        result = main_fill(parser, args)

        # Verify early return
        assert result == 0

    @patch('woom.cli.setup_workflow')
    @patch('woom.cli.wrender.JINJA_ENV.get_template')
    def test_main_fill_handles_exceptions(self, mock_get_template, mock_setup):
        """Test that main_fill handles exceptions gracefully"""
        # Setup mocks
        mock_workflow = Mock()
        mock_logger = Mock()
        mock_setup.return_value = (mock_workflow, mock_logger)

        # Make template retrieval raise an exception
        mock_get_template.side_effect = Exception("Template not found")

        # Create argument namespace
        parser = Mock()
        args = argparse.Namespace(
            template='nonexistent.j2',
            destination='/tmp/output.txt',
            task_name=None,
            cycle=None,
            member=None,
            dry_run=False,
        )

        # Execute - should handle the exception and return 1
        result = main_fill(parser, args)

        # Verify error code returned
        assert result == 1
        # Verify exception was logged
        mock_logger.exception.assert_called_once_with("Failed to fill template")


class TestGetParser:
    """Test get_parser function"""

    def test_get_parser_has_fill_subcommand(self):
        """Test that main parser includes fill subcommand"""
        parser = get_parser()

        # Parse with fill subcommand
        args = parser.parse_args(['fill', 'template.j2', 'output.txt'])

        assert args.template == 'template.j2'
        assert args.destination == 'output.txt'

    def test_get_parser_fill_with_global_options(self):
        """Test that fill subcommand can use global options"""
        parser = get_parser()

        # Parse with global options before subcommand
        args = parser.parse_args(
            [
                '--workflow-cfg',
                'custom.cfg',
                '--tasks-cfg',
                'custom_tasks.cfg',
                'fill',
                'template.j2',
                'output.txt',
            ]
        )

        assert args.workflow_cfg == 'custom.cfg'
        assert args.tasks_cfg == 'custom_tasks.cfg'
        assert args.template == 'template.j2'
        assert args.destination == 'output.txt'
