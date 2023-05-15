#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Environment loading utilities
"""
import os


class EnvConfig:
    def __init__(
        self,
        module_setup=None,
        module_use=None,
        module_load=None,
        vars_forward=None,
        vars_set=None,
        vars_append=None,
        vars_prepend=None,
    ):
        self.module_setup = module_setup
        self.module_use = module_use
        self.module_load = module_load
        self.vars_forward = vars_forward
        self.vars_set = vars_set
        self.vars_append = vars_append
        self.vars_prepend = vars_prepend

    def export_module(self):
        """Export env module call as bash lines"""
        if not self.module_load:
            return ""
        cmds = [self.module_setup]
        if self.module_use:
            cmds.append("module use " + self.module_use)
        cmds.append("module load " + self.module_load)
        return "\n".join(cmds) + "\n\n"

    def export_vars(self):
        """Export env var declarations as bash lines"""
        cmds = []
        if self.vars_forward:
            for vname in self.vars_forward:
                cmds.append(f"export {vname}=" + os.environ[vname])
        if self.vars_set:
            for vname, value in self.vars_set.items():
                cmds.append(f"export {vname}=" + value)
        if self.vars_append:
            for vname, value in self.vars_set.items():
                cmds.append(f"export {vname}=${vname}" + os.pathsep + value)
        if self.vars_prepend:
            for vname, value in self.vars_set.items():
                cmds.append(f"export {vname}=" + value + os.pathsep + "${vname}")
        return "\n".join(cmds) + "\n\n"

    def __str__(self):
        return self.export_module() + self.export_vars()
