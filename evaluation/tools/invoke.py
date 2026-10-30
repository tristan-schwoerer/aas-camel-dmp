"""Closed-loop Operation caller: one call at a time, on a schedule.

Call k of a segment is due at ``segment start + k / rate`` (tools/common.py ``scheduled``); it starts when it
is due or, if the previous call took longer, as soon as that one returned (its lateness is recorded). Targets:

- ``aas``   the Operation through the repository, ``POST …/submodel-elements/<path>/invoke``; BaSyx delegates
            it synchronously to the URL in the Operation's ``invocationDelegation`` qualifier
- ``dmp``   that delegation URL directly, with the body BaSyx sends it (an ``OperationVariable[]``)
- ``mqtt``  the device's action itself, as MQTT 5 request/reply (ResponseTopic, CorrelationData)
- ``http``  the device's action itself, a POST whose response is the reply
- ``opcua`` the device's Method itself, an OPC UA Call (input ``Request``, output ``Reply``)

The request is the string ``<seq>|<tSend>|xx…`` of exactly ``--size`` bytes; a call succeeded when its reply
carries it back. ``t_send_ns`` is taken after the request was built, just before it is sent, ``t_done_ns``
when the reply was read whole.

Writes ``calls.csv``: seq, phase, t_sched_ns, t_send_ns, t_done_ns, status, ok.
"""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import socket
import sys
import threading
import time
from urllib.parse import urlsplit

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from tools.common import Rows, scheduled, stop_event

TIMEOUT_S = 60


def request_value(seq: int, t_send: int, size: int) -> str:
    head = f"{seq}|{t_send}|"
    if size < len(head):
        raise ValueError(f"--size {size} is below the request's head ({len(head)} bytes)")
    return head + "x" * (size - len(head))


def _property(value: str) -> dict:
    return {"value": {"modelType": "Property", "idShort": "Request", "valueType": "xs:string", "value": value}}


class Http:
    """One kept-alive connection; ``body(value)`` builds what a call sends."""

    def __init__(self, url: str, body):
        parts = urlsplit(url)
        self.path, self.body = parts.path, body
        self.connection = http.client.HTTPConnection(parts.hostname, parts.port or 80, timeout=TIMEOUT_S)

    def call(self, value: str, seq: int) -> tuple[int, str, int, str]:
        body = json.dumps(self.body(value))
        t_send = time.time_ns()
        try:
            self.connection.request("POST", self.path, body, {"Content-Type": "application/json"})
            response = self.connection.getresponse()
            text = response.read()
            t_done = time.time_ns()
        except (OSError, http.client.HTTPException) as error:
            self.connection.close()
            return t_send, type(error).__name__, 0, ""
        return t_send, str(response.status), t_done, text.decode("utf-8", "replace")

    def close(self) -> None:
        self.connection.close()


class Mqtt:
    """MQTT 5 request/reply on one connection: the reply topic is subscribed before the first call."""

    def __init__(self, host: str, port: int, topic: str, response_topic: str, qos: int):
        self.topic, self.response_topic, self.qos = topic, response_topic, qos
        self.replies: dict[bytes, tuple[int, str]] = {}
        self.arrived = threading.Condition()
        subscribed = threading.Event()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="dmp-eval-invoke", protocol=mqtt.MQTTv5)
        self.client.on_connect = self._on_connect
        self.client.on_subscribe = lambda *_: subscribed.set()
        self.client.on_message = self._on_message
        self.client.connect(host, port, keepalive=60)
        self.client.loop_start()
        if not subscribed.wait(TIMEOUT_S):
            raise SystemExit("invoke: the broker did not acknowledge the reply subscription")

    def _on_connect(self, client, *_) -> None:
        client.socket().setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # a request goes out at once
        client.subscribe(self.response_topic, self.qos)

    def _on_message(self, client, userdata, message) -> None:
        t_done = time.time_ns()
        correlation = getattr(message.properties, "CorrelationData", None)
        with self.arrived:
            self.replies[bytes(correlation or b"")] = (t_done, message.payload.decode("utf-8", "replace"))
            self.arrived.notify_all()

    def call(self, value: str, seq: int) -> tuple[int, str, int, str]:
        correlation = str(seq).encode()
        properties = Properties(PacketTypes.PUBLISH)
        properties.ResponseTopic = self.response_topic
        properties.CorrelationData = correlation
        properties.ContentType = "application/json"
        payload = json.dumps({"Request": value})
        t_send = time.time_ns()
        self.client.publish(self.topic, payload, qos=self.qos, properties=properties)
        with self.arrived:
            if not self.arrived.wait_for(lambda: correlation in self.replies, TIMEOUT_S):
                return t_send, "timeout", 0, ""
            t_done, text = self.replies.pop(correlation)
        return t_send, "reply", t_done, text

    def close(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()


class Opcua:
    """OPC UA Calls of the device's Method on one session."""

    def __init__(self, url: str, owner: str, method: str):
        from asyncua import ua
        from asyncua.sync import Client
        self.client = Client(url, timeout=TIMEOUT_S)
        self.client.connect()
        # A NodeId, parsed: call_method reads a plain string as a browse path.
        self.owner, self.method = self.client.get_node(owner), ua.NodeId.from_string(method)

    def call(self, value: str, seq: int) -> tuple[int, str, int, str]:
        t_send = time.time_ns()
        try:
            reply = self.owner.call_method(self.method, value)
        except Exception as error:  # noqa: BLE001 (a failed call is recorded, the schedule goes on)
            return t_send, type(error).__name__, 0, ""
        return t_send, "reply", time.time_ns(), str(reply)

    def close(self) -> None:
        self.client.disconnect()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", choices=("aas", "dmp", "mqtt", "http", "opcua"), required=True)
    parser.add_argument("--repository", default="aas-env:8081", help="aas: host:port")
    parser.add_argument("--submodel", help="aas: the Operation's submodel id")
    parser.add_argument("--path", help="aas: the Operation's idShortPath")
    parser.add_argument("--url", help="dmp, http: where to POST; opcua: the server's endpoint")
    parser.add_argument("--object", help="opcua: the NodeId of the Object owning the Method")
    parser.add_argument("--method", help="opcua: the Method's NodeId")
    parser.add_argument("--host", default="mosquitto")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topic", help="mqtt: the action's request topic")
    parser.add_argument("--response-topic", help="mqtt: the action's response topic")
    parser.add_argument("--qos", type=int, default=1)
    parser.add_argument("--size", type=int, required=True, help="bytes of the request value")
    parser.add_argument("--seq-start", type=int, default=0, help="the first call's seq (readiness canaries: -1)")
    parser.add_argument("--schedule", required=True, help="JSON list of {phase, rate, seconds}")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    schedule = json.loads(args.schedule)
    request_value(args.seq_start, time.time_ns(), args.size)  # fail early on a size below the head
    stopped = stop_event()
    if args.target == "aas":
        submodel = base64.urlsafe_b64encode(args.submodel.encode()).decode().rstrip("=")
        url = f"http://{args.repository}/submodels/{submodel}/submodel-elements/{args.path}/invoke"
        caller = Http(url, lambda value: {"inputArguments": [_property(value)], "inoutputArguments": [],
                                          "clientTimeoutDuration": f"PT{TIMEOUT_S}S"})
    elif args.target == "dmp":
        caller = Http(args.url, lambda value: [_property(value)])
    elif args.target == "http":
        caller = Http(args.url, lambda value: {"Request": value})
    elif args.target == "opcua":
        caller = Opcua(args.url, args.object, args.method)
    else:
        caller = Mqtt(args.host, args.port, args.topic, args.response_topic, args.qos)

    rows = Rows(args.out, ["seq", "phase", "t_sched_ns", "t_send_ns", "t_done_ns", "status", "ok"])
    seq, failed = args.seq_start, 0
    try:
        for phase, due in scheduled(schedule, stopped):
            value = request_value(seq, time.time_ns(), args.size)
            t_send, status, t_done, text = caller.call(value, seq)
            # The reply must carry the request back: its value has nothing JSON would escape.
            ok = status in ("200", "reply") and value in text
            failed += not ok
            rows.write((seq, phase, due, t_send, t_done if t_done else "", status, int(ok)))
            seq += 1
    finally:
        rows.close()
        caller.close()
    print(f"invoke: {seq - args.seq_start} calls of {args.size} B to {args.target}, {failed} failed", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
