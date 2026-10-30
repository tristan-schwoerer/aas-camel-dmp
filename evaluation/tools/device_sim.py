"""The device of the southbound scenarios: it takes the writes and commands that reach
it, records when each arrived, and answers actions at once.

- MQTT 5, QoS 1: a write is a publish on a scenario's ``dmp-eval/<scenario>/setpoint`` topic; a command a
  publish on ``dmp-eval/<scenario>/echo`` with a ResponseTopic and CorrelationData, answered on that topic
  with the same correlation data and ``{"Reply": <Request>}``.
- HTTP, port 8090: ``PUT /<scenario>/setpoint`` is a write, answered 204; ``POST /<scenario>/echo`` a
  command, answered 200 with the reply.
- OPC UA, port 4840 (SecurityPolicy None, anonymous): a write is a Write of the String variable
  ``ns=2;s=Setpoint``, recorded as the server receives it (before it is applied); a command a Call of the
  Method ``ns=2;s=Echo`` on the Object ``ns=2;s=Device``, input argument ``Request``, output ``Reply``.

The same device for every path (a system under test, or the native client of tools/loadgen.py and
tools/invoke.py), so its own time cancels out of every comparison. Each value is ``<seq>|<tSend>|xx…``
(a command carries it as ``Request``), so a row names the write or call it belongs to.

Writes ``device.csv``: seq, t_send_ns, t_recv_ns, kind, protocol, bytes. ``t_recv_ns`` is taken when the
message was read whole, before anything is parsed. The ready file is written once the subscriptions are
acknowledged and the HTTP and OPC UA servers listen.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from asyncua import Server, ua
from asyncua.common.callback import CallbackType

from tools.common import Rows, device_value, stop_event

WRITE_TOPIC = "dmp-eval/+/setpoint"
COMMAND_TOPIC = "dmp-eval/+/echo"
OPCUA_NAMESPACE = "urn:dmp-eval:device"  # registered first, so it is ns=2 (fixtures/scenarios.py)


def request_of(body: bytes) -> str | None:
    """The ``Request`` of a command, ``{"Request": "<seq>|<tSend>|…"}``."""
    try:
        command = json.loads(body)
    except ValueError:
        return None
    return command.get("Request") if isinstance(command, dict) else None


def reply_to(request: str | None) -> bytes:
    return json.dumps({"Reply": request}).encode()


class Device:
    def __init__(self, out: str):
        self.rows = Rows(out, ["seq", "t_send_ns", "t_recv_ns", "kind", "protocol", "bytes"])
        self.unreadable = 0

    def record(self, t_recv: int, kind: str, protocol: str, value, size: int) -> None:
        message = device_value(value) if value is not None else None
        if message is None:
            self.unreadable += 1
            return
        self.rows.write((message[0], message[1], t_recv, kind, protocol, size))


def mqtt_device(device: Device, host: str, port: int, subscribed: threading.Event) -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="dmp-eval-device", protocol=mqtt.MQTTv5)
    client.max_inflight_messages_set(1000)
    client.max_queued_messages_set(0)

    def on_connect(client, userdata, flags, reason, properties):
        client.socket().setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # a reply goes out at once
        client.subscribe([(WRITE_TOPIC, 1), (COMMAND_TOPIC, 1)])

    def on_subscribe(client, userdata, mid, reasons, properties):
        subscribed.set()

    def on_message(client, userdata, message):
        t_recv = time.time_ns()
        if message.topic.endswith("/setpoint"):
            device.record(t_recv, "write", "mqtt", message.payload, len(message.payload))
            return
        request = request_of(message.payload)
        response_topic = getattr(message.properties, "ResponseTopic", None)
        if response_topic:
            properties = Properties(PacketTypes.PUBLISH)
            properties.CorrelationData = getattr(message.properties, "CorrelationData", b"")
            properties.ContentType = "application/json"
            client.publish(response_topic, reply_to(request), qos=1, properties=properties)
        device.record(t_recv, "invoke", "mqtt", request, len(message.payload))

    client.on_connect, client.on_subscribe, client.on_message = on_connect, on_subscribe, on_message
    client.connect(host, port, keepalive=60)
    client.loop_start()
    return client


def http_device(device: Device, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"  # keeps connections alive, as clients reuse them
        disable_nagle_algorithm = True

        def _body(self) -> bytes:
            """The request body, sized or chunked (the DMP's HTTP client streams its bodies chunked)."""
            if "chunked" not in self.headers.get("Transfer-Encoding", "").lower():
                return self.rfile.read(int(self.headers.get("Content-Length") or 0))
            body = bytearray()
            while True:
                size = int(self.rfile.readline().split(b";", 1)[0].strip() or b"0", 16)
                if size == 0:
                    while self.rfile.readline() not in (b"\r\n", b"\n", b""):  # trailers, up to the empty line
                        pass
                    return bytes(body)
                body += self.rfile.read(size)
                self.rfile.readline()  # the CRLF after each chunk

        def _answer(self, status: int, body: bytes = b"") -> None:
            self.send_response(status)
            if body:
                self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_PUT(self):
            body = self._body()
            t_recv = time.time_ns()
            if not self.path.endswith("/setpoint"):
                return self._answer(404)
            self._answer(204)
            device.record(t_recv, "write", "http", body, len(body))

        def do_POST(self):
            body = self._body()
            t_recv = time.time_ns()
            if not self.path.endswith("/echo"):
                return self._answer(404)
            request = request_of(body)
            self._answer(200, reply_to(request))
            device.record(t_recv, "invoke", "http", request, len(body))

        def log_message(self, *args) -> None:
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _argument(name: str) -> ua.Argument:
    argument = ua.Argument()
    argument.Name, argument.DataType, argument.ValueRank = name, ua.NodeId(ua.ObjectIds.String), -1
    return argument


def opcua_device(device: Device, host: str, port: int, ready: threading.Event,
                 stopped: threading.Event) -> threading.Thread:
    """The OPC UA server, on an event loop of its own thread."""
    async def serve() -> None:
        server = Server()
        await server.init()
        server.set_endpoint(f"opc.tcp://{host}:{port}/")
        server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        index = await server.register_namespace(OPCUA_NAMESPACE)
        assert index == 2, f"the device's namespace is ns={index}, the scenarios name ns=2"
        owner = await server.nodes.objects.add_object(ua.NodeId("Device", index), ua.QualifiedName("Device", index))
        setpoint = await owner.add_variable(ua.NodeId("Setpoint", index), ua.QualifiedName("Setpoint", index), "",
                                            varianttype=ua.VariantType.String)
        await setpoint.set_writable()

        def on_write(event, dispatcher) -> None:
            t_recv = time.time_ns()
            for write in event.request_params.NodesToWrite:
                if write.NodeId == setpoint.nodeid:
                    value = write.Value.Value.Value
                    device.record(t_recv, "write", "opcua", value, len(value or ""))

        async def echo(parent, request: ua.Variant) -> list[ua.Variant]:
            device.record(time.time_ns(), "invoke", "opcua", request.Value, len(request.Value or ""))
            return [ua.Variant(request.Value, ua.VariantType.String)]

        await owner.add_method(ua.NodeId("Echo", index), ua.QualifiedName("Echo", index), echo,
                               [_argument("Request")], [_argument("Reply")])
        server.subscribe_server_callback(CallbackType.PreWrite, on_write)
        async with server:
            ready.set()
            while not stopped.is_set():
                await asyncio.sleep(0.2)

    logging.getLogger("asyncua").setLevel(logging.ERROR)
    thread = threading.Thread(target=asyncio.run, args=(serve(),), daemon=True)
    thread.start()
    return thread


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="mosquitto")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--http-port", type=int, default=8090)
    parser.add_argument("--opcua-host", default="device-sim", help="the name the OPC UA server advertises")
    parser.add_argument("--opcua-port", type=int, default=4840)
    parser.add_argument("--out", required=True)
    parser.add_argument("--ready", required=True, help="file written once the device takes writes and commands")
    args = parser.parse_args()
    stopped = stop_event()

    device = Device(args.out)
    subscribed = threading.Event()
    client = mqtt_device(device, args.host, args.port, subscribed)
    server = http_device(device, args.http_port)
    opcua_ready, opcua_stop = threading.Event(), threading.Event()
    opcua = opcua_device(device, args.opcua_host, args.opcua_port, opcua_ready, opcua_stop)
    if not subscribed.wait(60):
        raise SystemExit("device_sim: the broker did not acknowledge the subscriptions")
    if not opcua_ready.wait(60):
        raise SystemExit("device_sim: the OPC UA server did not start")
    Path(args.ready).write_text("ready\n")
    try:
        stopped.wait()
    finally:
        server.shutdown()
        opcua_stop.set()
        opcua.join(timeout=10)
        client.loop_stop()
        client.disconnect()
        device.rows.close()
    print(f"device_sim: {device.unreadable} unreadable values", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
