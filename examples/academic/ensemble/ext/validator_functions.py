#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extend validator functions
"""
import random


def random_lognormal(specs):
    if str(specs) == "None":
        return
    mu = float(specs[0])
    sigma = float(specs[1])
    size = int(specs[2])
    return [random.lognormvariate(mu, sigma) for i in range(size)]


VALIDATOR_FUNCTIONS = {"random_lognormal": random_lognormal}
