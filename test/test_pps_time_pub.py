#!/usr/bin/env python3
"""
src/pps_time_pub/test/test_pps_time_pub.py

Unit tests for pps_time_pub.PpsTimePub.

pps_tools is mocked at the sys.modules level so the node module can be
imported without the real package, and patched per-test at the point of use
(pps_time_pub.pps_time_pub.pps_tools) so the patches actually reach the node.

Run (with workspace sourced):
    pytest src/pps_time_pub/test/test_pps_time_pub.py -v
"""

import sys
import time
import types
import threading
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Inject a fake pps_tools into sys.modules BEFORE importing the node so that
# "import pps_tools" inside pps_time_pub.py resolves without the real package.
# Force-assign (not setdefault) so we override any real installation.
# ---------------------------------------------------------------------------
_pps_data = types.ModuleType("pps_tools.data")
_pps_data.PPS_CAPTUREASSERT = 0x01

_pps_tools = types.ModuleType("pps_tools")
_pps_tools.data = _pps_data
_pps_tools.PpsFile = MagicMock

sys.modules["pps_tools"] = _pps_tools
sys.modules["pps_tools.data"] = _pps_data

import rclpy  # noqa: E402
from rclpy.executors import SingleThreadedExecutor  # noqa: E402
from builtin_interfaces.msg import Time  # noqa: E402

from pps_time_pub.pps_time_pub import PpsTimePub  # noqa: E402

# The module object the node imported — we patch pps_tools inside it directly.
import pps_time_pub.pps_time_pub as _node_mod  # noqa: E402

_PPS_TOOLS_PATH = "pps_time_pub.pps_time_pub.pps_tools"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeEdge:
    """Mimics the dict-like edge object returned by pps_tools 3.22 PpsFile.fetch()."""
    def __init__(self, sec: int, nsec: int):
        self._assert_time = sec + nsec / 1e9

    def __getitem__(self, key):
        if key == "assert_time":
            return self._assert_time
        raise KeyError(key)


class _SpyLogger:
    """Captures node log calls — ROS2 logging doesn't route through caplog."""
    def __init__(self):
        self.records = []

    def info(self, msg):  self.records.append(('info',  msg))
    def warn(self, msg):  self.records.append(('warn',  msg))
    def error(self, msg): self.records.append(('error', msg))
    def debug(self, msg): self.records.append(('debug', msg))
    def fatal(self, msg): self.records.append(('fatal', msg))

    def any_contains(self, substr):
        return any(substr in msg for _, msg in self.records)

    def warn_count(self, substr):
        return sum(1 for lvl, msg in self.records if lvl == 'warn' and substr in msg)


def _make_node(watchdog_interval_s: float = 10.0):
    """Manually init a PpsTimePub without starting the background thread."""
    node = PpsTimePub.__new__(PpsTimePub)
    rclpy.node.Node.__init__(node, "pps_time_pub_test")
    node.pub = node.create_publisher(Time, "/pps/time", 10)

    node.declare_parameter("pps_device", "/dev/pps0")
    node.declare_parameter("pps_topic", "/pps/time")
    node.declare_parameter("watchdog_interval_s", watchdog_interval_s)

    node.pps_device = node.get_parameter("pps_device").value
    node.pps_topic = node.get_parameter("pps_topic").value
    node.watchdog_interval = watchdog_interval_s
    node.stop_evt = threading.Event()

    return node


def _start(node):
    node.th = threading.Thread(target=node._run, daemon=True)
    node.th.start()
    return node.th


def _spin_for(node, secs: float):
    executor = SingleThreadedExecutor()
    executor.add_node(node)
    deadline = time.monotonic() + secs
    while time.monotonic() < deadline and rclpy.ok():
        executor.spin_once(timeout_sec=0.05)
    executor.remove_node(node)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def ros_context():
    rclpy.init()
    yield
    rclpy.shutdown()


@pytest.fixture()
def collector():
    node = rclpy.create_node("pps_collector")
    messages = []
    node.create_subscription(Time, "/pps/time", messages.append, 10)
    yield node, messages
    node.destroy_node()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPublishPath:

    def test_publishes_correct_timestamps(self, collector):
        """Each fake PPS edge must produce a Time message with matching sec/nanosec."""
        edges = [FakeEdge(1_000_000 + i, i * 1_000_000) for i in range(5)]
        n = {"i": 0}

        def fake_fetch(timeout=None):
            if n["i"] < len(edges):
                edge = edges[n["i"]]
                n["i"] += 1
                return edge
            time.sleep(0.02)
            return None

        with patch(_PPS_TOOLS_PATH) as mock_pps:
            mock_pps.data.PPS_CAPTUREASSERT = 0x01
            mock_pps.PpsFile.return_value.get_params.return_value = {"mode": 0}
            mock_pps.PpsFile.return_value.fetch.side_effect = fake_fetch

            node = _make_node()
            _start(node)

            collector_node, messages = collector
            time.sleep(0.3)
            _spin_for(collector_node, 1.0)

            node.stop_evt.set()
            node.th.join(timeout=2.0)
            node.destroy_node()

        assert len(messages) >= len(edges), (
            f"Expected {len(edges)} messages, got {len(messages)}"
        )
        for i, (msg, edge) in enumerate(zip(messages, edges)):
            assert_time = edge["assert_time"]
            expected_sec = int(assert_time)
            expected_nanosec = int((assert_time - expected_sec) * 1e9)
            assert msg.sec == expected_sec, \
                f"msg[{i}].sec {msg.sec} != {expected_sec}"
            assert msg.nanosec == expected_nanosec, \
                f"msg[{i}].nanosec {msg.nanosec} != {expected_nanosec}"

    def test_ppsfile_opened_with_configured_device(self):
        """PpsFile must be instantiated with the node's pps_device parameter."""
        with patch(_PPS_TOOLS_PATH) as mock_pps:
            mock_pps.data.PPS_CAPTUREASSERT = 0x01
            mock_pps.PpsFile.return_value.fetch.side_effect = lambda timeout=None: None

            node = _make_node()
            _start(node)
            time.sleep(0.2)
            node.stop_evt.set()
            node.th.join(timeout=2.0)
            node.destroy_node()

        mock_pps.PpsFile.assert_called_once_with(node.pps_device)


class TestErrorHandling:

    def test_open_failure_exits_cleanly(self):
        """If PpsFile() raises, the thread must exit — not hang."""
        with patch(_PPS_TOOLS_PATH) as mock_pps:
            mock_pps.data.PPS_CAPTUREASSERT = 0x01
            mock_pps.PpsFile.side_effect = OSError("no such device")

            node = _make_node()
            spy = _SpyLogger()
            node.get_logger = lambda: spy
            _start(node)
            node.th.join(timeout=3.0)
            node.destroy_node()

        assert not node.th.is_alive(), "Thread should have exited after open failure"
        assert spy.any_contains("Failed to open") or spy.any_contains("no such device"), (
            f"Expected error log. Got: {spy.records}"
        )

    def test_no_publish_when_fetch_returns_none(self, collector):
        """When fetch always returns None no messages should be published."""
        with patch(_PPS_TOOLS_PATH) as mock_pps:
            mock_pps.data.PPS_CAPTUREASSERT = 0x01
            mock_pps.PpsFile.return_value.fetch.side_effect = lambda timeout=None: None

            node = _make_node()
            _start(node)

            collector_node, messages = collector
            time.sleep(0.3)
            _spin_for(collector_node, 0.5)

            node.stop_evt.set()
            node.th.join(timeout=2.0)
            node.destroy_node()

        assert len(messages) == 0, (
            f"Expected 0 messages when fetch returns None, got {len(messages)}"
        )

    def test_fetch_exception_does_not_crash_node(self):
        """Transient fetch errors must be logged and the loop must continue."""
        n = {"i": 0}

        def flaky_fetch(timeout=None):
            n["i"] += 1
            if n["i"] % 3 == 0:
                raise IOError("transient error")
            time.sleep(0.02)
            return None

        with patch(_PPS_TOOLS_PATH) as mock_pps:
            mock_pps.data.PPS_CAPTUREASSERT = 0x01
            ppsf = mock_pps.PpsFile.return_value
            ppsf.get_params.return_value = {"mode": 0}
            ppsf.fetch.side_effect = flaky_fetch

            node = _make_node()
            _start(node)
            time.sleep(0.4)
            node.stop_evt.set()
            node.th.join(timeout=2.0)
            node.destroy_node()

        assert not node.th.is_alive()
        assert n["i"] > 3, f"Expected fetch to be called >3 times, got {n['i']}"


class TestWatchdog:

    def test_watchdog_warns_when_no_edges(self):
        """Watchdog must fire within watchdog_interval_s when no edges arrive."""
        def slow_none(timeout=None):
            time.sleep(0.05)
            return None

        with patch(_PPS_TOOLS_PATH) as mock_pps:
            mock_pps.data.PPS_CAPTUREASSERT = 0x01
            ppsf = mock_pps.PpsFile.return_value
            ppsf.get_params.return_value = {"mode": 0}
            ppsf.fetch.side_effect = slow_none

            node = _make_node(watchdog_interval_s=0.3)
            spy = _SpyLogger()
            node.get_logger = lambda: spy
            _start(node)
            time.sleep(1.0)
            node.stop_evt.set()
            node.th.join(timeout=2.0)
            node.destroy_node()

        assert spy.warn_count("PPS") >= 1, (
            f"Expected at least one watchdog warning. Got: {spy.records}"
        )

    def test_watchdog_throttled_not_every_fetch(self):
        """Watchdog must not fire on every None — only once per watchdog_interval_s."""
        n = {"i": 0}

        def fast_none(timeout=None):
            n["i"] += 1
            return None

        with patch(_PPS_TOOLS_PATH) as mock_pps:
            mock_pps.data.PPS_CAPTUREASSERT = 0x01
            ppsf = mock_pps.PpsFile.return_value
            ppsf.get_params.return_value = {"mode": 0}
            ppsf.fetch.side_effect = fast_none

            node = _make_node(watchdog_interval_s=10.0)
            spy = _SpyLogger()
            node.get_logger = lambda: spy
            _start(node)
            time.sleep(0.3)
            node.stop_evt.set()
            node.th.join(timeout=2.0)
            node.destroy_node()

        warn_count = spy.warn_count("PPS")
        assert warn_count <= 1, (
            f"Watchdog fired {warn_count} times in 0.3s with a 10s interval. "
            f"fetch called {n['i']} times. Records: {spy.records}"
        )


class TestShutdown:

    def test_thread_exits_on_destroy(self):
        """Background thread must finish within 3 s of destroy_node()."""
        def slow_fetch(timeout=None):
            time.sleep(0.05)
            return None

        with patch(_PPS_TOOLS_PATH) as mock_pps:
            mock_pps.data.PPS_CAPTUREASSERT = 0x01
            ppsf = mock_pps.PpsFile.return_value
            ppsf.get_params.return_value = {"mode": 0}
            ppsf.fetch.side_effect = slow_fetch

            node = _make_node()
            _start(node)
            time.sleep(0.2)
            node.destroy_node()

        assert not node.th.is_alive(), "Thread still alive after destroy_node()"

    def test_stop_evt_set_on_destroy(self):
        """stop_evt must be set by destroy_node() so the loop exits."""
        with patch(_PPS_TOOLS_PATH) as mock_pps:
            mock_pps.data.PPS_CAPTUREASSERT = 0x01
            ppsf = mock_pps.PpsFile.return_value
            ppsf.get_params.return_value = {"mode": 0}
            ppsf.fetch.side_effect = lambda timeout=None: None

            node = _make_node()
            _start(node)
            time.sleep(0.1)
            node.destroy_node()

        assert node.stop_evt.is_set(), "stop_evt must be set after destroy_node()"
