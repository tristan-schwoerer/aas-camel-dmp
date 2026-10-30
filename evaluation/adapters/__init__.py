"""One adapter per system under test.

An experiment never branches on the system: it asks the adapter. To add a
system, add a module here with an :class:`~adapters.base.Adapter` subclass,
register it in :data:`ADAPTERS`, and give it a profile in the testbed's Compose
file (a system without a container, as the PATCH reference, has none).
"""

from adapters.base import Adapter
from adapters.databridge import DataBridgeAdapter
from adapters.dmp import DmpAdapter
from adapters.native import NativeAdapter
from adapters.nodered_dmp import NoderedDmpAdapter
from adapters.patch import PatchAdapter

ADAPTERS: dict[str, type[Adapter]] = {"dmp": DmpAdapter, "databridge": DataBridgeAdapter,
                                      "nodered-dmp": NoderedDmpAdapter, "patch": PatchAdapter,
                                      "native": NativeAdapter}
