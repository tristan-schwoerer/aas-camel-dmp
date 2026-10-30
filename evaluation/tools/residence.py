"""Residence (O5): how long each message spends inside the system under test.

A packet capture in the system's network namespace (``samples/sut.pcap``, tcpdump, nanosecond time
stamps, the first bytes of every packet) sees the device message arrive from the broker and the
system's request to the repository leave. For each message:

- ``t_in_ns``: the packet that carried the message's last byte arrived (the system can only act then);
- ``t_request_ns``: the system sent the first byte of its first request to the repository for it;
- ``t_write_ns``: the first byte of the request that wrote the value (PATCH or PUT), when that is a
  second request (nodered-dmp reads the element first);
- ``t_ack_ns``: the first byte of the sink's answer to that write reached the system (the write is
  acknowledged; with the generator's tSend, the end-to-end latency of sinks that emit no BaSyx event);

so ``t_request_ns − t_in_ns`` is the time the message spends in the system, the repository's answer and
the broker left out, the same way for every system and every sink. Nothing in the systems is
instrumented: the capture needs no cooperation from them.

Messages are found from TCP sequence numbers, so a capture that keeps only the first bytes of each packet
suffices: an MQTT PUBLISH's fixed header gives its length, an HTTP request's headers its Content-Length.
A request is matched to its message by the ``seq`` in its body where the captured bytes reach it, else in
order (every system here handles a mapping's messages in order); an answer to its request in order on the
same connection (HTTP/1.1, no pipelining). Sinks: the AAS repository (8081) and InfluxDB 2 (8086, line
protocol, whose string field carries the extract scenario's ``<seq>|…`` value).

A batching sink (the DMP's InfluxDB segments written ``Asynchronous``) sends many messages in one request,
one line-protocol line each. A run is batched when a request shows more than one ``seq``. Batches keep the
messages' order and each body starts with its first message, whose ``seq`` the capture shows: a message is
taken by the request whose captured bytes show its ``seq``, else (``matched`` = ``batch``) by the last request
whose first ``seq`` is not above its own. Its ``t_request_ns`` is then when its batch left, so the residence
includes the wait in the buffer.

    python -m tools.residence samples/sut.pcap samples/residence.csv

Writes ``residence.csv``: seq, t_in_ns, t_request_ns, t_write_ns, t_ack_ns, method, matched (seq | order).
"""

from __future__ import annotations

import csv
import re
import struct
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

MQTT_PORT = 1883
REPOSITORY_PORT = 8081
INFLUXDB_PORT = 8086
SINK_PORTS = (REPOSITORY_PORT, INFLUXDB_PORT)
# "seq": N in a JSON message or body (quotes possibly escaped), or the extract scenario's "N|<tSend>|… value.
SEQ = re.compile(rb'seq\\*"\s*:\s*(\d+)|\\?"(\d+)\|\d+\|')


@dataclass
class Segment:
    ts: int        # ns since the epoch
    start: int     # stream offset of the first payload byte (relative to the flow's first seen sequence number)
    length: int    # payload bytes in the packet (on the wire)
    data: bytes    # the captured part of the payload


@dataclass
class Flow:
    segments: list[Segment] = field(default_factory=list)
    base: int | None = None
    end: int = 0   # highest stream offset covered so far

    def add(self, ts: int, seq: int, length: int, data: bytes, syn: bool) -> None:
        if self.base is None:
            self.base = seq + (1 if syn else 0)
        if length == 0:
            return
        start = (seq - self.base) % (1 << 32)
        if start + length <= self.end:
            return  # a retransmission of what was seen
        self.segments.append(Segment(ts, start, length, data))
        self.end = max(self.end, start + length)

    def bytes_at(self, offset: int, count: int) -> bytes | None:
        """Up to ``count`` captured bytes from ``offset``, if a segment's captured part holds that offset."""
        for segment in self._around(offset):
            inner = offset - segment.start
            if 0 <= inner < len(segment.data):
                return segment.data[inner:inner + count]
        return None

    def time_of(self, offset: int) -> int | None:
        """When the byte at ``offset`` arrived or left."""
        for segment in self._around(offset):
            if segment.start <= offset < segment.start + segment.length:
                return segment.ts
        return None

    def _around(self, offset: int):
        # Segments are in stream order; a binary search finds the one that may hold the offset.
        lo, hi = 0, len(self.segments)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.segments[mid].start + self.segments[mid].length <= offset:
                lo = mid + 1
            else:
                hi = mid
        return self.segments[lo:lo + 2]

    def segment_starts(self) -> list[int]:
        return [segment.start for segment in self.segments]


def read_pcap(path: Path):
    """(ts_ns, packet bytes, original length) of every record; nanosecond or microsecond pcap."""
    with open(path, "rb") as file:
        header = file.read(24)
        magic = struct.unpack("<I", header[:4])[0]
        if magic in (0xA1B23C4D, 0xA1B2C3D4):
            endian = "<"
        else:
            endian = ">"
            magic = struct.unpack(">I", header[:4])[0]
        nano = magic == 0xA1B23C4D
        linktype = struct.unpack(endian + "I", header[20:24])[0]
        while True:
            record = file.read(16)
            if len(record) < 16:
                return
            seconds, fraction, captured, original = struct.unpack(endian + "IIII", record)
            data = file.read(captured)
            yield seconds * 1_000_000_000 + (fraction if nano else fraction * 1000), data, original, linktype


def flows(path: Path) -> dict[tuple, Flow]:
    """TCP payload per direction: (src ip, src port, dst ip, dst port) -> Flow."""
    result: dict[tuple, Flow] = defaultdict(Flow)
    for ts, packet, original, linktype in read_pcap(path):
        offset = {1: 14, 113: 16, 276: 20}.get(linktype)  # Ethernet, Linux cooked v1 and v2
        if offset is None:
            raise ValueError(f"unsupported link type {linktype}")
        ethertype = struct.unpack(">H", packet[offset - 2:offset])[0]
        if ethertype != 0x0800:
            continue
        ip = packet[offset:]
        ihl = (ip[0] & 0x0F) * 4
        total = struct.unpack(">H", ip[2:4])[0]
        if ip[9] != 6:
            continue
        tcp = ip[ihl:]
        sport, dport, seq = struct.unpack(">HHI", tcp[:8])
        thl = (tcp[12] >> 4) * 4
        flags = tcp[13]
        length = total - ihl - thl
        result[(ip[12:16], sport, ip[16:20], dport)].add(ts, seq, length, tcp[thl:thl + max(0, length)], bool(flags & 0x02))
    return result


def mqtt_publishes(flow: Flow) -> list[tuple[int | None, int, int]]:
    """(seq, first byte time, last byte time) of every PUBLISH in a broker → system stream."""
    found = []
    offset, starts = 0, flow.segment_starts()
    while offset < flow.end:
        header = flow.bytes_at(offset, 5)
        if header is None or len(header) < 2:
            # The packet starts outside the captured bytes: continue at the next segment.
            nxt = next((s for s in starts if s > offset), None)
            if nxt is None:
                break
            offset = nxt
            continue
        remaining, shift, used = 0, 0, 1
        for byte in header[1:5]:
            remaining |= (byte & 0x7F) << shift
            used += 1
            shift += 7
            if not byte & 0x80:
                break
        total = used + remaining
        if header[0] >> 4 == 3:
            head = flow.bytes_at(offset, 512) or b""
            match = SEQ.search(head)
            first, last = flow.time_of(offset), flow.time_of(offset + total - 1)
            if first is not None and last is not None:
                found.append((_seq(match), first, last))
        offset += total
    return found


def http_responses(flow: Flow | None) -> list[int]:
    """First byte time of every final answer (not 1xx) in a sink → system stream. An answer starts a packet
    of its own: the system sends its next request only after the previous answer."""
    if flow is None:
        return []
    found = []
    for segment in flow.segments:
        status = re.match(rb"HTTP/1\.[01] (\d{3})", segment.data)
        if status and not status.group(1).startswith(b"1"):
            found.append(segment.ts)
    return found


def http_requests(flow: Flow) -> list[tuple[str, int | None, int, list[int]]]:
    """(method, seq in the body or None, first byte time, every seq the captured body shows) of every request in a
    system → repository stream."""
    found = []
    offset, starts = 0, flow.segment_starts()
    while offset < flow.end:
        head = flow.bytes_at(offset, 4096)
        end = head.find(b"\r\n\r\n") if head else -1
        if not head or end < 0 or not re.match(rb"[A-Z]+ ", head):
            nxt = next((s for s in starts if s > offset), None)
            if nxt is None:
                break
            offset = nxt
            continue
        method = head.split(b" ", 1)[0].decode()
        length = re.search(rb"(?i)\r\ncontent-length:\s*(\d+)", head[:end + 2])
        body_start = offset + end + 4
        body = flow.bytes_at(body_start, 1024) or b""
        match = SEQ.search(body)
        body_end = body_start + (int(length.group(1)) if length else 0)
        found.append((method, _seq(match), flow.time_of(offset), _visible_seqs(flow, body_start, body_end)))
        offset = body_end
    return found


def _visible_seqs(flow: Flow, start: int, end: int) -> list[int]:
    """Every seq in the captured bytes of the stream between ``start`` and ``end``, in stream order."""
    seqs = []
    for segment in flow.segments:
        if segment.start + segment.length <= start or segment.start >= end:
            continue
        lo = max(0, start - segment.start)
        hi = min(len(segment.data), end - segment.start)
        seqs.extend(_seq(match) for match in SEQ.finditer(segment.data[lo:hi]))
    return seqs


def _seq(match) -> int | None:
    if match is None:
        return None
    return int(match.group(1) or match.group(2))


def residence(path: Path) -> list[dict]:
    all_flows = flows(path)
    messages = sorted((m for key, f in all_flows.items() if key[1] == MQTT_PORT for m in mqtt_publishes(f)),
                      key=lambda m: m[2])
    requests = []
    for key, f in all_flows.items():
        if key[3] in SINK_PORTS:
            answers = http_responses(all_flows.get((key[2], key[3], key[0], key[1])))
            for i, (method, seq, ts, seqs) in enumerate(http_requests(f)):
                requests.append((method, seq, ts, answers[i] if i < len(answers) else None, seqs))
    requests.sort(key=lambda r: r[2])
    messages = [m for m in messages if m[0] is not None and m[0] >= 0]  # readiness canaries carry seq -1
    by_seq = {m[0]: m for m in messages}
    if any(len(set(r[4])) > 1 for r in requests):
        return _batched(messages, by_seq, [r for r in requests if r[0] != "GET"])
    requests = [r[:4] for r in requests]
    rows: dict[int, dict] = {}
    pending = [m[0] for m in messages]  # in arrival order, for requests without a readable seq
    reads: list[int] = []               # GETs waiting for the write they precede
    for method, seq, ts, ack in requests:
        if method == "GET":
            reads.append(ts)
            continue
        matched = "seq"
        if seq is None or seq not in by_seq:
            matched = "order"
            seq = next((s for s in pending if s not in rows), None)
            if seq is None:
                continue
        message = by_seq[seq]
        read = next((r for r in reads if r >= message[2]), None)
        if read is not None:
            reads = [r for r in reads if r > read]
        rows[seq] = {"seq": seq, "t_in_ns": message[2], "t_request_ns": read if read is not None else ts,
                     "t_write_ns": ts, "t_ack_ns": ack if ack is not None else "",
                     "method": method if read is None else f"GET+{method}", "matched": matched}
    return [rows[s] for s in sorted(rows)]


def _batched(messages: list, by_seq: dict, writes: list) -> list[dict]:
    """Messages of a batching sink. A batch keeps the messages' order and its body starts with its first message, whose
    seq the capture always shows: a message belongs to the request showing its seq, else to the last request whose
    first seq is not above its own. A request without a readable first seq (none in the runs so far) is skipped."""
    shown = {seq: i for i, write in enumerate(writes) for seq in write[4] if seq in by_seq}
    firsts = sorted((write[1], i) for i, write in enumerate(writes) if write[1] is not None and write[1] in by_seq)
    rows = []
    for seq, _, arrived in sorted(messages, key=lambda m: m[0]):
        matched, index = "seq", shown.get(seq)
        if index is None:
            matched = "batch"
            index = next((i for first, i in reversed(firsts) if first <= seq), None)
            if index is None:
                continue
        method, _, ts, ack, _ = writes[index]
        rows.append({"seq": seq, "t_in_ns": arrived, "t_request_ns": ts, "t_write_ns": ts,
                     "t_ack_ns": ack if ack is not None else "", "method": method, "matched": matched})
    return rows


def write(rows: list[dict], out: Path) -> None:
    with open(out, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["seq", "t_in_ns", "t_request_ns", "t_write_ns", "t_ack_ns", "method", "matched"])
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    rows = residence(Path(sys.argv[1]))
    write(rows, Path(sys.argv[2]))
    by_order = sum(1 for r in rows if r["matched"] in ("order", "batch"))
    print(f"residence: {len(rows)} messages ({by_order} matched by order or batch)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
