#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Custom validator functions for the tiles example.

``gen_tiles`` converts a compact ``nx, ny`` specification into the full list
of ``(x_label, y_label)`` tuples needed by the ensemble iterator.
"""

import string


def gen_tiles(value):
    """Generate tile coordinate pairs for an nx × ny grid.

    The config value must be ``nx, ny`` (two comma-separated integers).
    Returns a list of ``(x, y)`` tuples where x and y are uppercase letters.
    Tiles are ordered row by row (x varies fastest).

    Example: ``3, 2`` →
        ``[('A','A'), ('B','A'), ('C','A'), ('A','B'), ('B','B'), ('C','B')]``
    """
    if str(value) == "None":
        return []
    nx, ny = int(value[0]), int(value[1])
    return [(string.ascii_uppercase[i % nx], string.ascii_uppercase[i // nx]) for i in range(nx * ny)]


VALIDATOR_FUNCTIONS = {"gen_tiles": gen_tiles}
