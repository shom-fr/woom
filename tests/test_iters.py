#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for iters.py module
"""
import pytest

from woom import WoomError
from woom import iters as witers


class TestCycle:
    """Test Cycle class"""

    def test_cycle_single_date(self):
        cycle = witers.Cycle("2025-01-15")
        assert cycle.begin_date.year == 2025
        assert cycle.begin_date.month == 1
        assert cycle.begin_date.day == 15
        assert not cycle.is_interval
        assert cycle.end_date is None

    def test_cycle_interval(self):
        cycle = witers.Cycle("2025-01-15", "2025-01-20")
        assert cycle.is_interval
        assert cycle.duration.days == 5
        assert cycle.end_date.day == 20

    def test_cycle_token_single(self):
        cycle = witers.Cycle("2025-01-15")
        assert "2025-01-15" in cycle.token

    def test_cycle_token_interval(self):
        cycle = witers.Cycle("2025-01-15", "2025-01-20")
        assert "2025-01-15" in cycle.token
        assert "2025-01-20" in cycle.token

    def test_cycle_label(self):
        cycle = witers.Cycle("2025-01-15", "2025-01-20")
        assert "2025-01-15" in cycle.label
        assert "2025-01-20" in cycle.label

    def test_cycle_hash(self):
        cycle1 = witers.Cycle("2025-01-15")
        cycle2 = witers.Cycle("2025-01-15")
        assert hash(cycle1) == hash(cycle2)

    def test_cycle_get_params(self):
        cycle = witers.Cycle("2025-01-15", "2025-01-20")
        params = cycle.get_params()
        assert "cycle" in params
        assert "cycle_begin_date" in params
        assert "cycle_end_date" in params
        assert "cycle_duration" in params
        assert "cycle_token" in params

    def test_cycle_get_params_suffix(self):
        cycle = witers.Cycle("2025-01-15")
        params = cycle.get_params(suffix="prev")
        assert "cycle_prev" in params
        assert "cycle_begin_date_prev" in params

    def test_cycle_get_env_vars(self):
        cycle = witers.Cycle("2025-01-15")
        env_vars = cycle.get_env_vars()
        assert any(key.startswith("WOOM_CYCLE") for key in env_vars)


class TestGenCycles:
    """Test cycle generation"""

    def test_gen_cycles_single_date(self):
        cycles = witers.gen_cycles("2025-01-15")
        assert len(cycles) == 1
        assert cycles[0].begin_date.day == 15

    def test_gen_cycles_two_dates(self):
        cycles = witers.gen_cycles("2025-01-15", "2025-01-20")
        assert len(cycles) == 1
        assert cycles[0].is_interval
        assert cycles[0].begin_date.day == 15
        assert cycles[0].end_date.day == 20

    def test_gen_cycles_with_freq(self):
        cycles = witers.gen_cycles("2025-01-01", "2025-01-05", freq="1D")
        assert len(cycles) == 4
        assert all(c.is_interval for c in cycles)

    def test_gen_cycles_with_ncycles(self):
        cycles = witers.gen_cycles("2025-01-01", "2025-01-10", ncycles=3)
        assert len(cycles) == 3

    def test_gen_cycles_freq_and_ncycles(self):
        cycles = witers.gen_cycles("2025-01-01", freq="2D", ncycles=5)
        assert len(cycles) == 5
        assert cycles[0].duration.days == 2

    def test_gen_cycles_as_intervals_false(self):
        cycles = witers.gen_cycles("2025-01-01", "2025-01-03", freq="1D", as_intervals=False)
        assert len(cycles) == 3
        assert all(not c.is_interval for c in cycles)

    def test_gen_cycles_first_last_flags(self):
        cycles = witers.gen_cycles("2025-01-01", "2025-01-05", freq="1D")
        assert cycles[0].is_first
        assert cycles[-1].is_last
        assert not cycles[1].is_first
        assert not cycles[1].is_last

    def test_gen_cycles_prev_next_links(self):
        cycles = witers.gen_cycles("2025-01-01", "2025-01-05", freq="1D")
        assert cycles[0].next == cycles[1]
        assert cycles[1].prev == cycles[0]
        assert cycles[0].prev is None
        assert cycles[-1].next is None

    def test_gen_cycles_no_begin_date(self):
        with pytest.raises(WoomError):
            witers.gen_cycles(None)


class TestMember:
    """Test Member class"""

    def test_member_creation(self):
        member = witers.Member(5, 10)
        assert member.id == 5
        assert member.nmembers == 10

    def test_member_label(self):
        member = witers.Member(5, 100)
        assert member.label == "member005"

    def test_member_rank(self):
        member = witers.Member(5, 100)
        assert member.rank == "005/100"

    def test_member_set_prop(self):
        member = witers.Member(1, 10)
        member.set_prop("temperature", 25.5)
        assert member.temperature == 25.5
        assert "temperature" in member.props

    def test_member_props(self):
        member = witers.Member(1, 10)
        member.set_prop("temp", 25)
        member.set_prop("pressure", 1013)
        props = member.props
        assert props["temp"] == 25
        assert props["pressure"] == 1013

    def test_member_params(self):
        member = witers.Member(3, 10)
        member.set_prop("value", 42)
        params = member.params
        assert params["member"] == member
        assert params["nmembers"] == 10
        assert params["value"] == 42

    def test_member_env_vars(self):
        member = witers.Member(1, 10)
        env_vars = member.env_vars
        assert "WOOM_MEMBER" in env_vars or any("MEMBER" in k for k in env_vars)


class TestGenEnsemble:
    """Test ensemble generation"""

    def test_gen_ensemble_simple(self):
        members = witers.gen_ensemble(5)
        assert len(members) == 5
        assert members[0].id == 1
        assert members[-1].id == 5

    def test_gen_ensemble_with_skip(self):
        members = witers.gen_ensemble(10, skip=[3, 5, 7])
        assert len(members) == 7
        assert all(m.id not in [3, 5, 7] for m in members)

    def test_gen_ensemble_with_iters(self):
        temps = [20, 21, 22, 23, 24]
        members = witers.gen_ensemble(5, temperature=temps)
        assert len(members) == 5
        assert members[0].temperature == 20
        assert members[-1].temperature == 24

    def test_gen_ensemble_multiple_iters(self):
        temps = [20, 21, 22]
        pressures = [1000, 1010, 1020]
        members = witers.gen_ensemble(3, temperature=temps, pressure=pressures)
        assert members[0].temperature == 20
        assert members[0].pressure == 1000
        assert members[2].temperature == 22
        assert members[2].pressure == 1020

    def test_gen_ensemble_iters_mismatch(self):
        temps = [20, 21]
        with pytest.raises(WoomError):
            witers.gen_ensemble(3, temperature=temps)

    def test_gen_ensemble_from_iters(self):
        temps = [20, 21, 22]
        members = witers.gen_ensemble(None, temperature=temps)
        assert len(members) == 3

    def test_gen_ensemble_zero_members(self):
        members = witers.gen_ensemble(0)
        assert len(members) == 0
