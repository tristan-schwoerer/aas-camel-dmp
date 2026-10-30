"""O7, the repository's change events: when a consumer (the DMP) can have the event of a southbound write at
the earliest, on the transport the point's DMP follows.

- ``kafka``  every write of the target Property is one ``SME_UPDATED`` event on ``submodel-events``, keyed by the
  submodel id. The consumer starts at the end of every partition.
- ``mqtt``   BaSyx's submodel-repository MQTT feature publishes a change on ``sm-repository/<repo>/submodels/
  <base64url id>/submodelElements/<idShortPath>/…``, the element or its value as the payload.

The time this observer received an event is its event time. The ready file is written once the consumer is
positioned or the subscription acknowledged, so the runner starts the load only after that.

Writes ``events.csv``: seq, t_send_ns, t_recv_ns, value_bytes.
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
import threading
import time
from pathlib import Path

from tools.common import Rows, device_value, stop_event


def kafka(args, record, stopped) -> None:
    from confluent_kafka import Consumer, TopicPartition
    consumer = Consumer({"bootstrap.servers": args.bootstrap, "group.id": f"dmp-eval-events-{time.time_ns()}",
                         "enable.auto.commit": False, "fetch.max.bytes": 64 * 1024 * 1024,
                         "max.partition.fetch.bytes": 16 * 1024 * 1024, "fetch.wait.max.ms": 10})
    while not stopped.is_set():
        topic = consumer.list_topics(args.topic, timeout=10).topics.get(args.topic)
        if topic is not None and topic.error is None and topic.partitions:
            break
        time.sleep(0.5)
    partitions = [TopicPartition(args.topic, p, consumer.get_watermark_offsets(TopicPartition(args.topic, p), timeout=10)[1])
                  for p in sorted(topic.partitions)]
    consumer.assign(partitions)
    Path(args.ready).write_text(json.dumps({p.partition: p.offset for p in partitions}))
    key = args.submodel.encode()
    try:
        while not stopped.is_set():
            message = consumer.poll(0.2)
            if message is None:
                continue
            t_recv = time.time_ns()
            if message.error() is not None:
                print(f"observe_events: {message.error()}", file=sys.stderr)
                continue
            if message.key() != key:
                continue
            event = json.loads(message.value())
            if event.get("type") == "SME_UPDATED" and event.get("smElementPath") == args.path:
                record(t_recv, (event.get("smElement") or {}).get("value"))
    finally:
        consumer.close()


def mqtt(args, record, stopped) -> None:
    import paho.mqtt.client as paho

    def event_value(payload: bytes):
        """The written value of an event: the element's ``value``, or the payload itself (a value-only event)."""
        try:
            event = json.loads(payload)
        except ValueError:
            return payload
        return event.get("value") if isinstance(event, dict) else event

    submodel = base64.urlsafe_b64encode(args.submodel.encode()).decode().rstrip("=")
    topic = f"sm-repository/+/submodels/{submodel}/submodelElements/{args.path}/#"
    subscribed = threading.Event()
    client = paho.Client(paho.CallbackAPIVersion.VERSION2, client_id="dmp-eval-events", protocol=paho.MQTTv5)
    client.on_connect = lambda client, *_: client.subscribe(topic, 1)
    client.on_subscribe = lambda *_: subscribed.set()
    client.on_message = lambda client, userdata, message: record(time.time_ns(), event_value(message.payload))
    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()
    if not subscribed.wait(60):
        raise SystemExit("observe_events: the broker did not acknowledge the subscription")
    Path(args.ready).write_text(topic + "\n")
    try:
        stopped.wait()
    finally:
        client.loop_stop()
        client.disconnect()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--transport", choices=("kafka", "mqtt"), required=True)
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--topic", default="submodel-events", help="kafka: BaSyx's submodel events")
    parser.add_argument("--host", default="mosquitto")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--submodel", required=True)
    parser.add_argument("--path", required=True, help="idShortPath of the target Property")
    parser.add_argument("--out", required=True)
    parser.add_argument("--ready", required=True, help="file written once the observer is positioned")
    args = parser.parse_args()
    stopped = stop_event()
    rows = Rows(args.out, ["seq", "t_send_ns", "t_recv_ns", "value_bytes"])
    unreadable = 0

    def record(t_recv: int, value) -> None:
        nonlocal unreadable
        device = device_value(value) if value is not None else None
        if device is None:
            unreadable += 1
            return
        rows.write((device[0], device[1], t_recv, len(value)))

    try:
        (kafka if args.transport == "kafka" else mqtt)(args, record, stopped)
    finally:
        rows.close()
    print(f"observe_events: {unreadable} unreadable events", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
