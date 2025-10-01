#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Environment loading utilities
"""
import os

from . import render as wrender


class EnvConfig:
    def __init__(
        self,
        raw_text=None,
        vars_forward=None,
        vars_set=None,
        vars_append=None,
        vars_prepend=None,
        module_setup=None,
        module_use=None,
        module_load=None,
        conda_setup=None,
        conda_activate=None,
        uv_venv=None,
    ):
        self.raw_text = raw_text
        self.vars_forward = [] if vars_forward is None else list(vars_forward)
        self.vars_set = {} if vars_set is None else vars_set.copy()
        self.vars_append = {}
        self.vars_prepend = {}
        if vars_append:
            self.append_paths(**vars_append)
        if vars_prepend:
            self.prepend_paths(**vars_prepend)
        self.module_setup = module_setup
        self.module_use = module_use
        self.module_load = module_load
        self.conda_setup = conda_setup
        self.conda_activate = conda_activate
        self.uv_venv = uv_venv

    @staticmethod
    def _as_string_(value):
        if isinstance(value, (list, tuple)):
            return os.pathsep.join([str(v) for v in value])
        return str(value)

    def has_vars(self):
        """Does this environment manage environment variables?"""
        return bool(
            self.vars_forward + list(self.vars_set) + list(self.vars_prepend) + list(self.vars_append)
        )

    @staticmethod
    def _check_path_(path):
        if isinstance(path, str):
            return path.split(os.pathsep)
        return list(path)

    def _update_path_(self, action, varname, path):
        container = getattr(self, "vars_" + action)
        current_paths = self._check_path_(container.setdefault(varname, []))
        more_paths = self._check_path_(path)
        container[varname] = current_paths + more_paths

    def append_paths(self, **paths):
        """Append paths to env variables"""
        for varname, path in paths.items():
            self._update_path_("append", varname, path)

    def prepend_paths(self, **paths):
        """Prepend paths to env variables"""
        for varname, path in paths.items():
            self._update_path_("prepend", varname, path)

    def set_paths(self, **paths):
        """Set paths in env variables"""
        for varname, path in paths.items():
            self._update_path_("set", varname, path)

    def render(self, params=None):
        """Render the environment with template :file:`env.sh`"""
        if params is None:
            params = {}
            nested = False
            # strict = False
        else:
            params = params.copy()
            nested = True
            # strict = True
        params.update({"os": os, "env": self})
        return wrender.render(wrender.JINJA_ENV.get_template("env.sh"), params, strict=True, nested=nested)

    def __str__(self):
        return self.render()

    def copy(self):
        return EnvConfig(
            raw_text=self.raw_text,
            vars_forward=self.vars_forward,
            vars_set=self.vars_set,
            vars_append=self.vars_append,
            vars_prepend=self.vars_prepend,
            module_setup=self.module_setup,
            module_use=self.module_use,
            module_load=self.module_load,
            conda_setup=self.conda_setup,
            conda_activate=self.conda_activate,
            uv_venv=self.uv_venv,
        )
