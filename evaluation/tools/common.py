"""Shared by the tools: CSV output that survives SIGTERM, the schedule, and the device message."""

from __future__ import annotations

import csv
import json
import re
import signal
import threading
import time
from pathlib import Path


class Rows:
    """A CSV file written row by row and flushed at least every half second, so a
    stopped tool leaves every row it took."""

    def __init__(self, path: str | Path, header: list[str]):
        self._file = open(path, "w", newline="")
        self._writer = csv.writer(self._file)
        self._writer.writerow(header)
        self._lock = threading.Lock()
        self._flushed = time.monotonic()

    def write(self, row) -> None:
        with self._lock:
            self._writer.writerow(row)
            if time.monotonic() - self._flushed > 0.5:
                self._file.flush()
                self._flushed = time.monotonic()

    def close(self) -> None:
        with self._lock:
            self._file.close()


def scheduled(schedule: list[dict], stopped: threading.Event):
    """(phase, due time in ns) of every message on a schedule ``[{phase, rate, seconds}, …]``, each yielded when it
    is due: message k of a segment is due at ``segment start + k / rate``. A late message (the caller took longer)
    is yielded at once and keeps the schedule, so a slow system never thins the load (no coordinated omission).
    ``rate`` 0 is a pause."""
    start = time.time_ns()
    for segment in schedule:
        rate, length = float(segment["rate"]), int(segment["seconds"] * 1e9)
        if rate > 0:
            period, k = 1e9 / rate, 0
            while not stopped.is_set():
                due = start + int(k * period)
                if due >= start + length:
                    break
                wait = (due - time.time_ns()) / 1e9
                if wait > 0:
                    time.sleep(wait)
                yield segment["phase"], due
                k += 1
        else:
            stopped.wait(max(0.0, (start + length - time.time_ns()) / 1e9))
        start += length
        if stopped.is_set():
            return


def stop_event() -> threading.Event:
    """Set on SIGTERM or SIGINT (`docker stop`, Ctrl-C)."""
    stopped = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopped.set())
    return stopped


EXTRACTED = re.compile(r"^(-?\d+)\|(\d+)\|")


def device_value(value) -> tuple[int, int] | None:
    """(seq, tSend) of a value as a system delivered it: ``<seq>|<tSend>|…``, possibly JSON-quoted
    (a ValueOnly read, a JSON body), up to twice."""
    for _ in range(3):
        text = value.decode("utf-8", "replace") if isinstance(value, bytes) else value
        if not isinstance(text, str):
            return None
        extracted = EXTRACTED.match(text)
        if extracted:
            return int(extracted.group(1)), int(extracted.group(2))
        try:
            value = json.loads(value)
        except ValueError:
            return None
    return None
