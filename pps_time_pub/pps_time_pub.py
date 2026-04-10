#!/usr/bin/env python3
import signal
import subprocess
import threading
import time

import pps_tools

import rclpy
from rclpy.node import Node
from builtin_interfaces.msg import Time


class PpsTimePub(Node):
    def __init__(self):
        super().__init__("pps_time_pub")
        self.pub = self.create_publisher(Time, "/pps/time", 10)

        # ---------- Parameters ----------
        self.declare_parameter("pps_device", "/dev/pps0")
        self.declare_parameter("pps_topic", "/pps/time")

        # How long (s) without a PPS edge before the watchdog fires
        self.declare_parameter("watchdog_interval_s", 10.0)

        self.pps_device = self.get_parameter("pps_device").value
        self.pps_topic = self.get_parameter("pps_topic").value
        self.watchdog_interval = float(
            self.get_parameter("watchdog_interval_s").value
        )

        if self.pps_topic != "/pps/time":
            self.pub = self.create_publisher(Time, self.pps_topic, 10)

        self.stop_evt = threading.Event()

        self.get_logger().info(
            f"pps_time_pub starting. device={self.pps_device} topic={self.pps_topic}"
        )

        self.th = threading.Thread(target=self._run, daemon=True)
        self.th.start()

    def _run(self):
        # Open PPS device directlys
        try:
            ppsf = pps_tools.PpsFile(self.pps_device)
        except Exception as e:
            self.get_logger().error(
                f"Failed to open {self.pps_device}: {e}\n"
                "Tip: add a udev rule so you don't need sudo:\n"
                '  KERNEL=="pps0", GROUP="dialout", MODE="0660"'
            )
            return

        with ppsf:
            self.get_logger().info(f"Opened {self.pps_device} — waiting for PPS events")

            saw_assert = False
            # Use -inf so the watchdog fires immediately on the first missed edge,
            # then throttles to once per watchdog_interval_s regardless of saw_assert.
            last_warn = float("-inf")
            last_edge = time.time()
            pub_count = 0

            while not self.stop_evt.is_set():
                try:
                    # 2-second timeout so we wake up and check stop_evt regularly
                    edge = ppsf.fetch(timeout=2)
                except Exception as e:
                    if self.stop_evt.is_set():
                        break
                    self.get_logger().warn(f"PPS fetch: {e}")
                    continue

                now = time.time()

                if edge is None:
                    if now - last_warn > self.watchdog_interval:
                        self.get_logger().warn(
                            "No PPS edge received. Is the PWM generator running?"
                            f" (last edge {now - last_edge:.1f}s ago)"
                        )
                        last_warn = now
                    continue

                saw_assert = True
                last_edge = now
                last_warn = float("-inf")  # reset so next gap triggers immediately

                t = Time()
#                self.get_logger().info(f'{edge["assert_time"]}')
                t.sec = int(edge["assert_time"])
                t.nanosec = int((edge["assert_time"] - int(edge["assert_time"])) * 1e9)
#                self.get_logger().info(f"t.sec: {t.sec},     t.nanosec: {t.nanosec}")
                self.pub.publish(t)

                pub_count += 1
                self.get_logger().debug(
                    f"PPS edge #{pub_count}: {t.sec}.{t.nanosec:09d}"
                )

        self.get_logger().info(f"PPS device {self.pps_device} closed")
        self.get_logger().info("PPS reader thread exiting")

    def destroy_node(self):
        self.stop_evt.set()
        self.th.join(timeout=3.0)
        super().destroy_node()


def main():
    rclpy.init()
    node = PpsTimePub()
    signal.signal(signal.SIGTERM, lambda *_: rclpy.shutdown())
    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, rclpy.executors.ExternalShutdownException):
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
