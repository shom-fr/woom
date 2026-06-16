# Bug #28 – Woom restarts all tasks when all jobs previously succeeded

## Summary

When **all** tasks completed successfully on a previous run, calling `woom run`
again (without `woom clean`) re-submits every task instead of skipping them.
In the mixed case (some failed, some succeeded), re-run correctly retries only
the failed tasks.

Observed on **Datarmor** (HPC with PBS Pro scheduler).

---

## Root cause – Bug 1: wrong identity comparison (primary)

**File:** `woom/workflow.py`, inside `Workflow.run()`

The condition that decides whether to skip an already-successful task was:

```python
if status.name is wjob.JobStatus.SUCCESS:   # BUG
    ...
    continue
```

`status.name` is the *string* `"SUCCESS"`.
`wjob.JobStatus.SUCCESS` is the `JobStatus` *Enum member*.

Python's `is` operator tests **object identity**, not value equality.  A string
object and an Enum member are never the same object, so this condition is
**always `False`** regardless of the actual task status.  The `continue` that
would skip re-submission is therefore unreachable, and every task — including
those that already succeeded — is cleaned and re-submitted.

The analogous checks on the next two lines used the correct form:

```python
elif status is wjob.JobStatus.ERROR:    # correct – Enum identity
elif status is wjob.JobStatus.UNKNOWN:  # correct – Enum identity
```

which is why the ERROR and UNKNOWN branches worked as expected.

### Why the mixed case appeared correct

When some tasks fail and others succeed, the failed tasks hit the correctly
working `elif status is wjob.JobStatus.ERROR` branch and are re-submitted.
Because the workflow is often structured sequentially (a failing task aborts
the scheduler chain before later tasks are submitted), later tasks may never
have a `job.status` file and are therefore seen as `NOTSUBMITTED` — also
correctly re-submitted.  The user may not notice that *already-succeeded*
parallel tasks are also being incorrectly re-run.

### Fix

```python
if status is wjob.JobStatus.SUCCESS:    # compare Enum to Enum
    ...
    continue
```

---

## Secondary issue – Bug 2: spurious sentinel re-submission

**File:** `woom/workflow.py`, `Workflow.get_task_status()` and `Workflow.run()`

`get_task_status()` calls `self.jobmanager.load_job(json_file, append=True)`,
which appends every previously-submitted job to `self.jobmanager.jobs`.  This
happens even for tasks whose status is SUCCESS and that are subsequently
*skipped* (never re-submitted).

At the end of `run()`, the scheduler path is:

```python
if self.jobmanager.with_scheduler:
    self.submit_sentinel()
```

`submit_sentinel()` checks `if not self.jobmanager.jobs` to decide whether to
skip.  Because `self.jobmanager.jobs` was populated by the `load_job()` calls
during status checks, it is non-empty even when **no new tasks were submitted**.
The sentinel is therefore re-submitted to PBS on every subsequent `woom run`,
consuming a job slot unnecessarily.

### Fix

Track the number of tasks actually submitted during the current run.  Only
submit the sentinel when at least one new job was dispatched.

```python
# In Workflow.run(), before the main loop:
n_submitted = 0

# After each submit_task() / submit_task_fake() call:
n_submitted += 1

# At the end of run():
if self.jobmanager.with_scheduler:
    if n_submitted:
        self.submit_sentinel()
else:
    self.terminate_blocking_jobs()
```

---

## Impact

| Scenario | Before fix | After fix |
|---|---|---|
| All tasks succeeded, re-run | All tasks re-submitted (wrong) | All tasks skipped (correct) |
| Some tasks failed, re-run | Failed + succeeded re-submitted¹ | Only failed tasks re-submitted |
| First run | Normal submission | Normal submission |
| All tasks succeeded, re-run (scheduler) | Unnecessary sentinel PBS job submitted | No sentinel submitted |

¹ In practice this may go unnoticed depending on workflow structure.

---

## Affected versions

All versions prior to the fix on the `develop` branch (bug present since the
`force`/`--update` logic was introduced).

## References

- Issue: #28
- Fix commit: see `fix(run): skip already-succeeded tasks when force=False`
  (Bug 1) and accompanying sentinel guard (Bug 2)
