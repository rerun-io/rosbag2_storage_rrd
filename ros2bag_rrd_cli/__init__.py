"""
CLI extension for the ``rrd`` rosbag2 storage plugin.

Surfaces the presets so ``ros2 bag record -s rrd --help`` lists them and
``--storage-preset-profile`` validates against them. Every preset records the
reflected representation; ``--storage-config-file`` chooses a different set. The README
documents its keys.
"""

from __future__ import annotations


def get_preset_profiles() -> list[tuple[str, str]]:
    """Return the ``(name, description)`` pairs this plugin accepts."""
    return [
        ("none", "Flush at 64 rows, 8 MiB, or 200 ms (default)"),
        ("low_latency", "Flush every frame, ~30 Hz max latency (live viewing)"),
        ("ultra_low_latency", "Flush every frame, ~60 Hz max latency"),
        ("high_throughput", "Flush at 1024 rows or 8 MiB, no latency cap (fast recording)"),
    ]
