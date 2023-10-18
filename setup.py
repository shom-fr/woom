#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup


with open('README.rst') as f:
    long_description = f.read()

setup(long_description=long_description, use_scm_version={"fallback_version": "999"})
