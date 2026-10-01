# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

import signal
import subprocess

from harness.common import wait_for


class DBusServer:
    """Test only DBus instance."""
    def __init__(self, root):
        """
        Create a new DBus environment, enabling each test to have its own
        bus and ensuring isolation.
        """
        self._socket = root / "dbus.sock"
        self._config = root / "dbus.xml"
        self._config.write_text(
            """<!DOCTYPE busconfig PUBLIC "-//freedesktop//DTD D-BUS Bus Configuration 1.0//EN"
 "http://www.freedesktop.org/standards/dbus/1.0/busconfig.dtd">
<busconfig>
  <type>system</type>
  <listen>unix:path=%s</listen>
  <auth>EXTERNAL</auth>
  <policy context="default">
    <allow own="*"/>
    <allow send_destination="*"/>
    <allow receive_sender="*"/>
  </policy>
</busconfig>
"""
            % self._socket
        )
        self._proc = None
        self.address = None

    def start(self):
        """Start the bus and wait for it to come up."""
        self._proc = subprocess.Popen(
            [
                "dbus-daemon",
                "--nofork",
                "--config-file=%s" % self._config,
                "--print-address",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert self._proc.stdout is not None
        self.address = self._proc.stdout.readline().strip()
        if not self.address:
            raise RuntimeError("dbus-daemon did not print an address")
        wait_for(self._socket.exists, 5, "DBus to come up")

    def stop(self):
        """Stop the bus."""
        if self._proc is None or self._proc.poll() is not None:
            return
        self._proc.send_signal(signal.SIGTERM)
        try:
            self._proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait(timeout=5)
