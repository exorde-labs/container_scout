"""
Initially this code has been written to help prometheus detect targets to scrap.

It evolved as a component of the stack with the "get" endpoint which is used by
scrapers to determin which upipe they should send data to. This avoids providing
the volume access for docker api to containers that are horizontaly scaled.
"""
import logging, os
from aiohttp import web

from handle_prometheus_targets import handle_targets
from handle_spotting_targets import handle_ips_by_label
from orchestrate_spotters import start_orchestrator

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
)

app = web.Application()
app.add_routes([web.get('/targets', handle_targets)])
app.add_routes([web.get('/get', handle_ips_by_label)])

ORCHESTRATE = os.getenv("ORCHESTRATE", False)
if int(ORCHESTRATE):
    logging.info(f"running orchestration : {ORCHESTRATE}")
    app.on_startup.append(start_orchestrator)
else:
    logging.info('NOT running orchestration')

if __name__ == '__main__':
    logging.info("Starting ContainerScout service on port 8080...")
    web.run_app(app, port=8080)
