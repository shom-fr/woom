Working with artifacts
======================

About
-----

This example demonstrates woom's artifact system for tracking, validating, and managing workflow output files.

Artifacts provide a structured way to handle files that tasks produce and consume, ensuring data lineage and enabling automatic validation. This example shows three common artifact patterns:

**1. Download artifact** (``download_clim`` task):

- Prolog task that downloads a climatology file
- Registers the file as an artifact named ``clim_file``
- Makes the artifact available to downstream tasks throughout the workflow
- Demonstrates artifact generation in setup stages

**2. Dynamic artifact generation** (``run_model`` task):

- Cyclic task that processes data and generates daily output files
- Uses the ``gen_daily_files`` custom function extension to create multiple artifacts per cycle
- Accesses the climatology artifact from the prolog stage
- Copies the last daily file from the previous cycle (``prev_croco_rst.nc``) for restart capability
- Shows inter-cycle artifact dependencies

**3. Artifact aggregation** (``concat_nc`` task):

- Epilog task that consolidates all daily artifacts from ``run_model``
- Merges multiple cycle outputs into a single ``merged`` artifact
- Demonstrates post-processing patterns for time series data

This example is essential for workflows that need to track data provenance, validate outputs, share files between tasks, or implement restart capabilities.
