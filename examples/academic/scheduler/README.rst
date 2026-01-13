With a scheduler
================

About
-----

This example demonstrates how to run workflows on HPC systems using a job scheduler, specifically PBS Pro.

Unlike local execution where tasks run as background processes, this example shows how to:

- **Configure a scheduler**: Set ``scheduler=pbspro`` in hosts.cfg to use PBS Pro
- **Submit jobs**: Tasks are submitted as batch jobs to the scheduler queue system
- **Specify resources**: Configure job requirements like memory (``mem=1GB``) and walltime (``time=01:00:00``)
- **Use different queues**: Route tasks to specific queues (e.g., ``queue=omp`` for OpenMP jobs)
- **Monitor with sentinel**: A special sentinel job continuously monitors the workflow status
- **Handle job failures**: Demonstrates workflow behavior when a job fails (mytask1 exits with error code 1)

The workflow consists of:

- **mytask1**: A job that fails after 10 seconds, demonstrating error handling
- **mytask2**: A longer-running job (120 seconds) on a specific queue
- **sentinel**: A monitoring job that tracks the workflow progress

This example is essential for users deploying workflows on HPC clusters with SLURM, PBS Pro, or other schedulers. The principles shown here apply to any scheduler configuration in woom.
