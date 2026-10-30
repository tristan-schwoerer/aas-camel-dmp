"""The PATCH reference: the repository write alone, which every system ends with.

No broker, no subscription, no parsing: the load generator (``--target patch``) writes each message's
sink value (aas-property's ``value`` member) straight into the target Property with ``PATCH …/$value`` on the
experiment's schedule, one at a time over one kept-alive connection, the value already built. Each
PATCH's round trip (``sent.csv``, ``t_done_ns``) is the repository's share of every system's latency.
"""

from __future__ import annotations

from adapters.base import Adapter, Repository, Scenario, REPOSITORY_INTERNAL


class PatchAdapter(Adapter):
    name = "patch"
    profile = None  # nothing to start: the load generator writes
    service = None

    def supports(self, scenario: Scenario) -> bool:
        return scenario.id == "aas-property"

    def prepare(self, run) -> dict:
        self.run = run
        return {"image": run.harness_image, "implementation": "tools/loadgen.py --target patch"}

    def deploy(self, scenario: Scenario, repository: Repository) -> None:
        # The same repository content as every other system.
        repository.post_environment(scenario.environment, last=scenario.aimc)

    def ready(self, scenario: Scenario) -> bool:
        return True

    def load_args(self, scenario: Scenario, variant: str | None = None) -> list[str]:
        return ["--target", "patch", "--repository", REPOSITORY_INTERNAL.removeprefix("http://"),
                "--submodel", scenario.target_submodel, "--path", scenario.target_path]
