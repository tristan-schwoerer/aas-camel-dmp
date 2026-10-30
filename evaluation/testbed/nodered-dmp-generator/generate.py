"""Run nodered-dmp's generator as its main.py does, with the server and AIMC id as arguments
(main.py hard-codes both): python generate.py <repository url> <AIMC id> > flow.json"""

import sys

from nodered_dmp.aas_client import id_to_url
from nodered_dmp.aimc_parser import get_connections
from nodered_dmp.flow_builder import build_flow

server, aimc_id = sys.argv[1], sys.argv[2]
connections = get_connections(id_to_url(server, aimc_id))
print(build_flow(connections, server).generate_json())
