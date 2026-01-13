Running all stages
==================

About
-----

This example demonstrates the complete workflow structure in woom by using all three stages: prolog, cycles, and epilog.

The workflow showcases:

- **Prolog stage**: Setup tasks (ptask0-3) that run once at the beginning
- **Cycles stage**: Processing tasks (ctask0-3) that run for each cycle (3 daily cycles)
- **Epilog stage**: Finalization tasks (etask0-3) that run once at the end
- **Task groups**: Reusable groups (group0, group1) that bundle multiple tasks together
- **Sequential execution**: Multiple sequences within each stage (prolog0→prolog1, cycles0→cycles1, etc.)
- **Parallel execution**: Tasks within each sequence run in parallel (comma-separated)
- **Independent cycles**: With ``indep=True``, all cycles can run simultaneously
- **Queue configuration**: Different tasks using different execution queues
- **Woom environment variables**: Tasks access cycle information via ``$WOOM_CYCLE_TOKEN``

This example is ideal for understanding how to structure complex workflows with multiple stages, organize task dependencies, and leverage parallelism.
