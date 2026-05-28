# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Constraints

Never commit without explicit permission from the user.
Propose short commit messages without mentioning Claude.

## Commands

```bash
# Install for development
pip install -e .[dev]

# Run all tests
pytest -v

# Run a single test
pytest tests/test_iters.py::TestCycle::test_cycle_single_date -v

# Lint
ruff check woom tests

# Format check (CI)
ruff format --check woom tests

# Auto-format
ruff format woom tests

# Build docs
cd docs && make html
```

## Architecture

woom is a workflow manager for ocean models. Users write three config files (`workflow.cfg`, `tasks.cfg`, `hosts.cfg`) and run `woom run` to submit batched job sequences to an HPC scheduler (Slurm, PBS Pro) or locally.

### Config system

All config files are INI-style, loaded and validated by `configobj` via `woom/conf.py`. Each config type has a matching `.ini` spec file (`workflow.ini`, `tasks.ini`, `hosts.ini`) inside `woom/`. The spec files declare types like `datetime`, `timedelta`, `path`, and `pages` which are custom validator functions registered in `conf.py:VALIDATOR_FUNCTIONS`.

### Core data flow

1. `Workflow.__init__` loads `workflow.cfg`, validates it, then calls `iters.gen_cycles()` and `iters.gen_ensemble()` to produce the `Cycle` and `Member` lists.
2. When running, the workflow loops over stages (`prolog` → `cycles` → `epilog`), then over `Cycle` objects and `Member` objects.
3. For each task/cycle/member combination, a `Context` dict (`context.py`) is built by merging: workflow config, host params, cycle params (`Cycle.get_params()`), member params, task params, and user `[params]` from `workflow.cfg`.
4. The context is passed to Jinja2 to render batch scripts and task template files.
5. The rendered script is submitted via a job manager (`job.py`: `SlurmJobManager`, `PbsproJobManager`, or `LocalJobManager`).

### Key classes

- `Cycle` (`iters.py`): Represents either a single date or a time interval. Has `begin_date`, `end_date`, `duration`, `is_interval`, `is_first`, `is_last`, `prev`, `next`. `end_date` is `None` when `as_intervals=False`.
- `gen_cycles()` (`iters.py`): Produces the list of `Cycle` objects from `workflow.cfg [cycles]` parameters.
- `Member` (`iters.py`): Represents an ensemble member with user-defined properties via `[[iters]]` subsection.
- `Workflow` (`workflow.py`): Central orchestrator. Holds cycles, members, task tree, and the context cache (`@functools.lru_cache` on `get_context()`).
- `Context` (`context.py`): A `UserDict` passed to Jinja2. Built fresh for each task/cycle/member. Includes all cycle params prefixed `cycle_*`, member params, task params, path helpers, etc.
- `TaskTree` / `TaskManager` (`tasks.py`): Parses `tasks.cfg` and the `[stages]` section of `workflow.cfg`. Returns task sequences and groups.

### Academic examples

Self-contained runnable examples live in `examples/academic/<name>/`. Each example is a directory containing at minimum `README.rst` and `workflow.cfg`; `tasks.cfg` and `hosts.cfg` are included when needed. A custom `example.rst` Jinja template can be added to override or extend the generated documentation page for that example.

During doc generation (`cd docs && make html`), the `docs/ext/genexamples.py` Sphinx extension scans `examples/academic/` and `examples/realistic/`, renders an RST page for each directory that has a `README.rst`, and writes it to `docs/examples/<section>/<name>.rst`. The default template (`docs/_templates/genexamples/example.rst`) runs `woom show overview`, `woom run --dry-run`, `woom run`, `woom show status`, and `woom show run_dirs` live via `sphinxcontrib-programoutput`, so examples must be fully runnable without a scheduler.

Reference an example from documentation with `:ref:\`examples.academic.<name>\``.

### Extensions

Workflows can extend woom by placing files in their `ext/` subdirectory:
- `ext/jinja_filters.py`: custom Jinja2 filters
- `ext/validator_functions.py`: additional configobj validator types
- `ext/cfgspecs/`: custom `.ini` specs merged with built-in ones

These are loaded by `ext.py:load_extensions()` at workflow startup.

### Cycle modes

The `[cycles]` section of `workflow.cfg` drives `gen_cycles()`:
- `as_intervals=True` (default): N dates → N-1 interval `Cycle` objects, each with `begin_date` and `end_date`.
- `as_intervals=False`: N dates → N point-in-time `Cycle` objects, `end_date=None`.
