Cycling on date intervals
=========================

About
-----

In this example, we loop of over date intervals.

``task0`` simply shows how to play with dates.

``task1`` shows how to use a file named :file:`result.txt` that was created in the last cycle.
It first gets the run directory of previous cycle using method :meth:`~woom.workflow.Workflow.get_run_dir`
and specifying the previous cycle with :attr:`cycle.prev <woom.iters.Cycle.prev>`.
Then it verifies that the file exists and copies it to the current run directory.
Eventually it append the :attr:`cycle.token <woom.iters.Cycle.token>` to the end of the file.
