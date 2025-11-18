Working with artifacts
======================

About
-----
This example show a simple use of artifacts.

* A prolog task named ``download_clim`` downloads a climatology to an artifact named ``clim_file``.
* A cyclic task named ``run_model`` uses this climatology artifact and create daily artifact files with the ``gen_daily_files`` function extension. It also copes the last daily file of the previous cycle run of itself to file ``prev_croco_rst.nc``.
* Finally, all artifacts of ``run_model`` are merged by task ``concat_nc`` into an artifact file named ``merged``.
