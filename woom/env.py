#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Environment loading utilities
"""
import os


class EnvConfig:
    def __init__(
        self,
        raw_text=None,
        module_setup=None,
        module_use=None,
        module_load=None,
        vars_forward=None,
        vars_set=None,
        vars_append=None,
        vars_prepend=None,
    ):
        self.raw_text = raw_text or ""
        self.module_setup = module_setup
        self.module_use = module_use
        self.module_load = module_load
        self.vars_forward = [] if vars_forward is None else list(vars_forward)
        self.vars_set = {} if vars_set is None else vars_set.copy()
        self.vars_append = {} if vars_forward is None else vars_append.copy()
        self.vars_prepend = {} if vars_prepend is None else vars_prepend.copy()

    def export_module(self):
        """Export env module call as bash lines"""
        if not self.module_load:
            return ""
        cmds = [self.module_setup]
        if self.module_use:
            cmds.append("module use " + self.module_use)
        cmds.append("module load " + self.module_load)
        return "\n# ENVIRONMENT MODULES\n" + "\n".join(cmds) + "\n"

    def export_vars(self):
        """Export env var declarations as bash lines"""
        cmds = []
        if self.vars_forward:
            for vname in self.vars_forward:
                cmds.append(f"export {vname}='" + os.environ[vname] + "'")
        if self.vars_set:
            for vname, value in self.vars_set.items():
                cmds.append(f"export {vname}='{value}'")
        if self.vars_append:
            for vname, value in self.vars_append.items():
                cmds.append(f"export {vname}=${vname}" + os.pathsep + value)
        if self.vars_prepend:
            for vname, value in self.vars_prepend.items():
                cmds.append(f"export {vname}=" + value + os.pathsep + f"${vname}")
        return "\n# ENVIRONMENT VARIABLES\n" + "\n".join(cmds) + "\n"

    def __str__(self):
        return self.raw_text + "\n" + self.export_module() + self.export_vars()

    def copy(self):
        return EnvConfig(
            raw_text=self.raw_text,
            module_setup=self.module_setup,
            module_use=self.module_use,
            module_load=self.module_load,
            vars_forward=self.vars_forward,
            vars_set=self.vars_set,
            vars_append=self.vars_append,
            vars_prepend=self.vars_prepend,
        )
