Forecast cycles with horizon
============================

About
-----

This example demonstrates the ``horizon`` option for date-based cycles (``as_intervals = False``).

Three initialization dates are defined (2020-01-01, 2020-01-02, 2020-01-03), each producing
a 5-day forecast. The ``horizon = 5D`` setting computes ``end_date = begin_date + 5 days``
for every cycle, making :attr:`~woom.iters.Cycle.end_date` and
:attr:`~woom.iters.Cycle.duration` available in templates via ``cycle_end_date`` and
``cycle_duration`` — without making :attr:`~woom.iters.Cycle.is_interval` True.

**forecast** task:

- Prints the initialization date (``cycle_begin_date``) and the forecast end date (``cycle_end_date``)
- Illustrates that a single-date cycle can carry a forecast window alongside its anchor date
- Cycles run independently (``indep = True``) since forecasts do not depend on each other
