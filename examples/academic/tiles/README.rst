Tiling a domain with ensemble members
======================================

About
-----

This example demonstrates how to distribute the processing of a 2-D spatial
grid of tiles across parallel ensemble members, combining woom's ensemble
feature with the configobj validator extension mechanism.

The workflow runs a single task once per tile.  The grid dimensions are the
only inputs the user provides: woom generates the full set of tile coordinates
automatically through a custom validator function.

Key concepts
------------

**Tile grid as an ensemble** :

Each tile ``(x, y)`` of the grid is mapped to one ensemble member.  With
``nx=3`` tiles along x and ``ny=2`` along y the workflow spawns 6 members in
parallel:

.. code-block:: text

   (A, A)  (B, A)  (C, A)
   (A, B)  (B, B)  (C, B)

**Custom validator function** (:file:`ext/validator_functions.py`):

The ``gen_tiles`` function converts the compact ``nx, ny`` value found in
``workflow.cfg`` into the full list of ``(x_label, y_label)`` tuples required
by the ensemble iterator.  It is registered through woom's validator extension
mechanism so that configobj applies it automatically during config validation.

**Custom workflow configspec** (:file:`workflow.ini`):

A local :file:`workflow.ini` file is merged with woom's built-in
configuration specification.  It overrides the default ``force_list`` validator
for the ``tile`` iter key with the custom ``gen_tiles`` validator, allowing the
config to stay compact:

.. code-block:: ini

   [[iters]]
   tile = 3, 2  # nx=3 tiles along x, ny=2 tiles along y

**Processing script** (:file:`bin/process_tile.py`):

A minimal Python script that receives the tile coordinates ``x`` and ``y`` as
command-line arguments.  Woom automatically adds the :file:`bin/` directory to
``$PATH``, so the task commandline can call it by name:

.. code-block:: ini

   commandline = process_tile.py {{ member.tile[0] }} {{ member.tile[1] }}
