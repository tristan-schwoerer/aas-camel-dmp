"""The testbed's Compose project and the harness image, driven from the host."""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

EVALUATION = Path(__file__).resolve().parent.parent
COMPOSE_FILE = EVALUATION / "testbed" / "docker-compose.yml"
PROJECT = "dmp-eval"
NETWORK = "dmp-eval"
INFRASTRUCTURE = ("mongo", "kafka", "aas-env", "mosquitto")
TOPICS = ("submodel-events", "aas-events")
HARNESS_IMAGE = "dmp-eval-harness:local"
# Cores per role (testbed/docker-compose.yml): the system under test, the infrastructure, the tools.
SUT_CPUSET = os.environ.get("SUT_CPUSET", "0-3")
INFRA_CPUSET = os.environ.get("INFRA_CPUSET", "4-7")
TOOLS_CPUSET = os.environ.get("TOOLS_CPUSET", "8-15")


def build_harness() -> str:
    """Build the harness image from evaluation/ and return its image id."""
    subprocess.run(["docker", "build", "-q", "-t", HARNESS_IMAGE, str(EVALUATION)], check=True,
                   stdout=subprocess.DEVNULL)
    return subprocess.run(["docker", "image", "inspect", "-f", "{{.Id}}", HARNESS_IMAGE], check=True,
                          capture_output=True, text=True).stdout.strip()


class Testbed:
    def __init__(self, ports: dict[str, int]):
        self.ports = ports
        self.env = {**os.environ, "HOST_UID": str(os.getuid()), "HOST_GID": str(os.getgid()),
                    "HARNESS_IMAGE": HARNESS_IMAGE,
                    **{f"EVAL_{name.upper()}_PORT": str(port) for name, port in ports.items()}}

    def compose(self, *args: str, profiles: tuple[str, ...] = ()) -> list[str]:
        command = ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE_FILE)]
        for profile in profiles:
            command += ["--profile", profile]
        return command + list(args)

    def _run(self, *args: str, profiles: tuple[str, ...] = (), env: dict | None = None, **kwargs):
        return subprocess.run(self.compose(*args, profiles=profiles), env={**self.env, **(env or {})},
                              check=True, **kwargs)

    def images(self) -> list[str]:
        """The image references of every service, as pinned in the Compose file."""
        out = self._run("config", "--images", profiles=("*",), capture_output=True, text=True).stdout
        return sorted(set(out.split()))

    def resolved_config(self) -> str:
        return self._run("config", profiles=("*",), capture_output=True, text=True).stdout

    def down(self) -> None:
        self._run("down", "-v", "--remove-orphans", "--timeout", "60", profiles=("*",),
                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def up_infrastructure(self, extra: tuple[str, ...] = (), env: dict[str, str] | None = None) -> None:
        """The shared infrastructure, and ``extra`` services a scenario needs (behind profiles of their name);
        ``env`` configures it for the point (southbound: where BaSyx publishes its events)."""
        self._run("up", "-d", "--wait", "--wait-timeout", "300", *INFRASTRUCTURE, *extra, profiles=extra, env=env,
                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # The topics exist before anyone subscribes, each with one partition, so every run starts alike.
        for topic in TOPICS:
            self._run("exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "kafka:9092", "--create",
                      "--if-not-exists", "--topic", topic, "--partitions", "1", "--replication-factor", "1",
                      stdout=subprocess.DEVNULL)

    def up_system(self, profile: str, service: str, env: dict[str, str]) -> None:
        self._run("up", "-d", "--wait", "--wait-timeout", "300", "--no-deps", service, profiles=(profile,), env=env,
                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def stop(self, profile: str, service: str, env: dict[str, str]) -> None:
        """After the measurement: nothing is taken from a stopping system, so it gets a short grace period
        (DataBridge ignores SIGTERM and would otherwise hold every point for the full timeout)."""
        self._run("stop", "--timeout", "5", service, profiles=(profile,), env=env,
                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def publish(self, topic: str, payload: str, qos: int) -> None:
        """One MQTT message from inside the broker's container (readiness canaries)."""
        self._run("exec", "-T", "mosquitto", "mosquitto_pub", "-q", str(qos), "-t", topic, "-m", payload,
                  stdout=subprocess.DEVNULL)

    def container(self, service: str) -> str:
        return f"{PROJECT}-{service}-1"

    def logs(self, out: Path, *profiles: str | None) -> None:
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w") as file:
            self._run("logs", "--no-color", "--timestamps", profiles=tuple(p for p in profiles if p), stdout=file,
                      stderr=subprocess.STDOUT)


class Tool:
    """One measurement tool in the harness image on the testbed network, writing into samples/."""

    def __init__(self, name: str, module: str, args: list[str], samples: Path, *, command: list[str] | None = None,
                 network: str = NETWORK, root: bool = False, options: tuple[str, ...] = ()):
        self.container = f"dmp-eval-{name}"
        user = [] if root else ["--user", f"{os.getuid()}:{os.getgid()}"]
        self.command = ["docker", "run", "--rm", "--name", self.container, "--network", network,
                        "--cpuset-cpus", TOOLS_CPUSET, *user, *options, "-v", f"{samples}:/out",
                        HARNESS_IMAGE, *(command or ["python", "-m", f"tools.{module}", *args])]

    @classmethod
    def capture(cls, container: str, samples: Path) -> "Tool":
        """tcpdump in ``container``'s network namespace: its MQTT, repository and InfluxDB traffic, the first 1500 bytes of
        every packet, nanosecond time stamps, into samples/sut.pcap (tools/residence.py reads it)."""
        return cls("capture", "", [], samples, network=f"container:{container}", root=True,
                   options=("--cap-add", "NET_RAW", "--cap-add", "NET_ADMIN"),
                   command=["tcpdump", "-i", "eth0", "-s", "1500", "-B", "32768", "-U", "-n", "--time-stamp-precision=nano",
                            "-w", "/out/sut.pcap", "tcp port 1883 or tcp port 8081 or tcp port 8086"])

    @staticmethod
    def own(samples: Path, name: str) -> None:
        """Hands a file a root tool wrote to the user running the harness."""
        subprocess.run(["docker", "run", "--rm", "-v", f"{samples}:/out", HARNESS_IMAGE, "chown",
                        f"{os.getuid()}:{os.getgid()}", f"/out/{name}"], capture_output=True)

    def start(self) -> "Tool":
        subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)
        subprocess.run(self.command[:2] + ["-d"] + self.command[2:], check=True, stdout=subprocess.DEVNULL)
        return self

    def run(self, timeout: float) -> None:
        subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)
        subprocess.run(self.command, check=True, timeout=timeout)

    def stop(self) -> None:
        subprocess.run(["docker", "stop", "--time", "20", self.container], capture_output=True)


def wait_for(predicate, timeout: float, what: str, interval: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError(f"{what} within {timeout:.0f} s")
        time.sleep(interval)
