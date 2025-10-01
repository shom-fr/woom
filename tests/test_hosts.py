#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for hosts.py module
"""
from unittest.mock import patch

import configobj
import pytest

from woom import env as wenv
from woom import hosts as whosts


class TestHostManager:
    """Test HostManager class"""

    def test_init(self):
        manager = whosts.HostManager()
        assert manager._config is not None
        assert isinstance(manager._config, configobj.ConfigObj)

    def test_config_property(self):
        manager = whosts.HostManager()
        config = manager.config
        assert isinstance(config, configobj.ConfigObj)

    def test_load_config(self, tmp_path):
        manager = whosts.HostManager()

        cfg_file = tmp_path / "custom_hosts.cfg"
        cfg_file.write_text(
            """
[myhost]
patterns = myhost*
scheduler = slurm
"""
        )

        manager.load_config(str(cfg_file))
        assert "myhost" in manager.config

    def test_get_host(self):
        manager = whosts.HostManager()
        host = manager.get_host("local")
        assert isinstance(host, whosts.Host)
        assert host.name == "local"

    @patch('socket.getfqdn')
    def test_infer_host_local(self, mock_getfqdn):
        mock_getfqdn.return_value = "unknown.host.com"
        manager = whosts.HostManager()
        host = manager.infer_host()
        assert host.name == "local"

    @patch('socket.getfqdn')
    def test_infer_host_pattern_match(self, mock_getfqdn):
        mock_getfqdn.return_value = "compute-node-01.cluster.fr"

        manager = whosts.HostManager()
        # Add a test host with pattern
        manager._config["testcluster"] = {
            "patterns": ["compute-node*.cluster.fr"],
            "scheduler": "slurm",
        }

        host = manager.infer_host()
        assert host.name == "testcluster"


class TestHost:
    """Test Host class"""

    def setup_method(self):
        """Setup for each test"""
        self.host_config = {
            "patterns": ["localhost"],
            "scheduler": "background",
            "module_setup": None,
            "conda_setup": None,
            "queues": {"seq": None, "omp": None},
            "dirs": {"scratch": "/scratch", "work": "/work"},
            "envs": {
                "default": {
                    "raw_text": None,
                    "conda_activate": None,
                    "modules": {"use": None, "load": None},
                    "vars": {"forward": [], "set": {}, "prepend": {}, "append": {}},
                }
            },
            "params": {},
        }

    def test_init(self):
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)
        assert host.name == "testhost"
        assert host._config == config

    def test_name_property(self):
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("myhost", config)
        assert host.name == "myhost"

    def test_str_representation(self):
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)
        assert str(host) == "testhost"

    def test_config_property(self):
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)
        assert isinstance(host.config, dict)

    def test_getitem(self):
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)
        assert host["scheduler"] == "background"

    def test_module_setup_property(self):
        self.host_config["module_setup"] = "source /etc/modules.sh"
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)
        assert host.module_setup == "source /etc/modules.sh"

    def test_queues_property(self):
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)
        queues = host.queues
        assert "seq" in queues
        assert "omp" in queues

    def test_get_queue_exists(self):
        self.host_config["queues"]["seq"] = "normal"
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)
        assert host.get_queue("seq") == "normal"

    def test_get_queue_not_exists(self):
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)
        assert host.get_queue("custom") == "custom"

    def test_get_params(self):
        self.host_config["dirs"]["scratch"] = "$HOME/scratch"
        self.host_config["dirs"]["work"] = "/work"
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)

        params = host.get_params()
        assert "scratch_dir" in params
        assert "work_dir" in params
        assert params["work_dir"] == "/work"

    def test_get_params_expands_vars(self):
        self.host_config["dirs"]["test"] = "$HOME/test"
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)

        params = host.get_params()
        assert "$HOME" not in params["test_dir"]

    def test_get_env_none(self):
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)

        env = host.get_env(None)
        assert isinstance(env, wenv.EnvConfig)

    def test_get_env_registered(self):
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)

        env = host.get_env("default")
        assert isinstance(env, wenv.EnvConfig)

    def test_get_env_invalid(self):
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)

        with pytest.raises(whosts.HostError):
            host.get_env("nonexistent")

    def test_get_env_with_dirs(self):
        self.host_config["dirs"]["scratch"] = "/scratch"
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)

        env = host.get_env("default")
        assert "WOOM_SCRATCH_DIR" in env.vars_set
        assert env.vars_set["WOOM_SCRATCH_DIR"] == "/scratch"

    def test_get_jobmanager(self):
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)

        manager = host.get_jobmanager()
        assert manager is not None

    def test_get_jobmanager_cached(self):
        config = configobj.ConfigObj(self.host_config)
        host = whosts.Host("testhost", config)

        manager1 = host.get_jobmanager()
        manager2 = host.get_jobmanager()
        assert manager1 is manager2


class TestHostConfiguration:
    """Test host configuration loading and validation"""

    def test_default_local_host(self):
        manager = whosts.HostManager()
        assert "local" in manager.config
        local = manager.config["local"]
        assert local["scheduler"] == "background"
