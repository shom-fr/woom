Cycling on date intervals
=========================

About
-----

This example demonstrates how to work with time cycles in woom, showing both date manipulation and inter-cycle dependencies.

The workflow runs 4 cycles over 6-hour intervals from 2020-01-01 to 2020-01-02, with two tasks:

**task0** - Date manipulation:

- Shows how to work with cycle dates using :attr:`cycle.begin_date <woom.iters.Cycle.begin_date>`
- Demonstrates date arithmetic with ``add()`` method (e.g., ``add('-10D')`` for 10 days earlier)
- Formats dates using ``strftime()`` for custom output formats

**task1** - Inter-cycle dependencies:

- Creates a file :file:`result.txt` that accumulates data across cycles
- On first cycle (:attr:`cycle.is_first <woom.iters.Cycle.is_first>`): Initializes the file
- On subsequent cycles: Copies the file from previous cycle using :attr:`cycle.prev <woom.iters.Cycle.prev>`
- Uses :meth:`workflow.get_task_run_dir() <woom.workflow.Workflow.get_task_run_dir>` to locate the previous cycle's run directory
- Appends the current :attr:`cycle.token <woom.iters.Cycle.token>` to track progression

This example demonstrates sequential cycle dependencies where each cycle builds on results from the previous one, essential for time-stepping models and iterative workflows.
