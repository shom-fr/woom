#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test the :mod:`woom.util` module
"""
import woom.util as wutil


def test_util_subst_dict():
    dd_in = {
        "a": "{b} aa",
        "c": "aa {d} {e}",
        "b": "xx {c}",
        "d": "dd",
        "x": 1,
    }
    dd_subst = {"d": "dss", "e": "ee"}
    dd_expected = {
        "a": "xx aa dd ee aa",
        "c": "aa dd ee",
        "b": "xx aa dd ee",
        "d": "dd",
        "x": 1,
    }
    dd_out = wutil.subst_dict(dd_in, dict_subst=dd_subst)
    assert dd_out == dd_expected
