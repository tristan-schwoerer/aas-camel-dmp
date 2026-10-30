"""Open-loop load generator.

Publishes the device message ``{"seq": n, "tSend": "<ns>", "value": "<n>|<ns>|xx…"}`` (``--format extract``,
the northbound scenarios) on a fixed schedule that never waits for the system under test (tools/common.py ``scheduled``); a late
publish's lateness is recorded.
Every message is exactly ``--size`` bytes. ``tSend`` is taken immediately before
the publish, in nanoseconds since the epoch, as a string (it exceeds 2^53).

With ``--target patch`` (the PATCH reference) nothing is published: each message's sink value (its
``value`` member) is written straight into the repository with ``PATCH …/$value``, one at a time over one kept-alive connection; ``t_send_ns`` is
then taken after the body was built, just before the request, and ``t_done_ns`` when the repository
answered. It measures the repository write alone, for the systems' figures to subtract.

``--format value`` is the southbound value: the JSON string ``"<seq>|<tSend>|xx…"``, what the
target Property holds and what every path delivers to the device. ``--target patch`` writes it into
the Property (a system takes it from there to the device); ``--target mqtt`` publishes it and
``--target http`` PUTs it and ``--target opcua`` Writes it (``--url``, ``--node``) to the device itself, the
native write a system's path is compared with.

The schedule is a JSON list of segments; ``rate`` 0 is a pause:

    [{"phase": "warmup", "rate": 10, "seconds": 30},
     {"phase": "measure", "rate": 10, "seconds": 120}]

Writes ``sent.csv``: seq, phase, t_sched_ns, t_send_ns, bytes, t_done_ns (all targets but ``mqtt``).
"""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import sys
import time
from urllib.parse import urlsplit

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from tools.common import Rows, scheduled, stop_event

# The device message: the value member names its message itself (seq|tSend|padding).
EXTRACT = '{"seq":%d,"tSend":"%d","value":"%d|%d|'
# The southbound value: a JSON string naming its message itself.
VALUE = '"%d|%d|'


def message(seq: int, t_send: int, size: int, pad: bytes, form: str = "extract") -> bytes:
    if form == "value":
        head, tail = (VALUE % (seq, t_send)).encode(), b'"'
    else:
        head = (EXTRACT % (seq, t_send, seq, t_send)).encode()
        tail = b'"}'
    filler = size - len(head) - len(tail)
    if filler < 0:
        raise ValueError(f"--size {size} is below the message envelope ({len(head) + len(tail)} bytes)")
    return head + pad[:filler] + tail


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="mosquitto")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topic", help="--target mqtt: where to publish")
    parser.add_argument("--qos", type=int, default=1)
    parser.add_argument("--size", type=int, required=True, help="bytes per message")
    parser.add_argument("--format", choices=("extract", "value"), default="extract")
    parser.add_argument("--target", choices=("mqtt", "patch", "http", "opcua"), default="mqtt")
    parser.add_argument("--repository", default="aas-env:8081", help="--target patch: host:port")
    parser.add_argument("--submodel", help="--target patch: the target submodel's id")
    parser.add_argument("--path", help="--target patch: the target Property's idShortPath")
    parser.add_argument("--url", help="--target http: where to PUT; --target opcua: the server's endpoint")
    parser.add_argument("--node", help="--target opcua: the NodeId of the variable to write")
    parser.add_argument("--seq-start", type=int, default=0, help="the first message's seq (readiness canaries: -1)")
    parser.add_argument("--schedule", required=True, help="JSON list of {phase, rate, seconds}")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    schedule = json.loads(args.schedule)
    pad = b"x" * args.size
    message(10 ** 9, time.time_ns(), args.size, pad, args.format)  # fail early on a size below the envelope
    stopped = stop_event()

    if args.target != "mqtt":
        return write(args, schedule, pad, stopped)
    if not args.topic:
        parser.error("--target mqtt needs --topic")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="dmp-eval-loadgen", protocol=mqtt.MQTTv5)
    client.max_inflight_messages_set(1000)
    client.max_queued_messages_set(0)
    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()
    properties = Properties(PacketTypes.PUBLISH)
    properties.ContentType = "application/json"

    rows = Rows(args.out, ["seq", "phase", "t_sched_ns", "t_send_ns", "bytes", "t_done_ns"])
    seq, last = args.seq_start, None
    try:
        for phase, due in scheduled(schedule, stopped):
            t_send = time.time_ns()
            payload = message(seq, t_send, args.size, pad, args.format)
            last = client.publish(args.topic, payload, qos=args.qos, properties=properties)
            rows.write((seq, phase, due, t_send, len(payload), ""))
            seq += 1
        if last is not None:
            last.wait_for_publish(timeout=60)
    finally:
        rows.close()
        client.loop_stop()
        client.disconnect()
    print(f"loadgen: {seq - args.seq_start} messages of {args.size} B on {args.topic}", file=sys.stderr)
    return 0


class HttpWriter:
    """PATCH into the repository (the PATCH reference, or a southbound write a system takes on), or PUT to the
    device itself, over one kept-alive connection."""

    def __init__(self, args):
        if args.target == "patch":
            host, _, port = args.repository.partition(":")
            submodel = base64.urlsafe_b64encode(args.submodel.encode()).decode().rstrip("=")
            self.name, self.target = "PATCH", f"/submodels/{submodel}/submodel-elements/{args.path}/$value"
        else:
            url = urlsplit(args.url)
            host, port, self.name, self.target = url.hostname, url.port, "PUT", url.path
        self.connection = http.client.HTTPConnection(host, int(port or 80), timeout=60)

    def send(self, value: str) -> tuple[int, int | str]:
        body = json.dumps(value)
        t_send = time.time_ns()
        try:
            self.connection.request(self.name, self.target, body, {"Content-Type": "application/json"})
            response = self.connection.getresponse()
            response.read()
            return t_send, time.time_ns() if response.status < 300 else ""
        except (OSError, http.client.HTTPException):
            self.connection.close()
            return t_send, ""

    def close(self) -> None:
        self.connection.close()


class OpcuaWriter:
    """The OPC UA Write service on the device's String variable, one session."""

    def __init__(self, args):
        from asyncua import ua
        from asyncua.sync import Client
        self.name, self.ua = "Write", ua
        self.client = Client(args.url, timeout=60)
        self.client.connect()
        self.node = self.client.get_node(args.node)

    def send(self, value: str) -> tuple[int, int | str]:
        data = self.ua.DataValue(self.ua.Variant(value, self.ua.VariantType.String))
        t_send = time.time_ns()
        try:
            self.node.write_value(data)
            return t_send, time.time_ns()
        except Exception:  # noqa: BLE001 (a failed write is recorded, the schedule goes on)
            return t_send, ""

    def close(self) -> None:
        self.client.disconnect()


def write(args, schedule: list[dict], pad: bytes, stopped) -> int:
    """Each message's sink value written on the schedule, by HTTP (``patch``, ``http``) or OPC UA (``opcua``);
    ``t_done_ns`` when the write was acknowledged."""
    writer = OpcuaWriter(args) if args.target == "opcua" else HttpWriter(args)
    rows = Rows(args.out, ["seq", "phase", "t_sched_ns", "t_send_ns", "bytes", "t_done_ns"])
    seq, failures = args.seq_start, 0
    try:
        for phase, due in scheduled(schedule, stopped):
            text = message(seq, time.time_ns(), args.size, pad, args.format).decode()
            # The value a system writes: the message's value member, or the value (plain text, no escapes).
            value = text[1:-1] if args.format == "value" else text[text.index('"value":"') + 9:-2]
            t_send, t_done = writer.send(value)
            failures += t_done == ""
            rows.write((seq, phase, due, t_send, len(text), t_done))
            seq += 1
    finally:
        rows.close()
        writer.close()
    print(f"loadgen: {seq - args.seq_start} {writer.name}s of {args.size} B messages, {failures} failed", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
