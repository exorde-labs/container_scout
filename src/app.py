"""
Initially this code has been written to help prometheus detect targets to scrap.

It evolved as a component of the stack with the "get" endpoint which is used by
scrapers to determin which upipe they should send data to. This avoids providing
the volume access for docker api to containers that are horizontaly scaled.
"""
import logging, os
from aiohttp import web

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
)

from handle_prometheus_targets import handle_targets
from handle_spotting_targets import handle_ips_by_label
from orchestrate_spotters import (
    start_orchestrator, delete_all_managed_containers
)
from update import (
    start_update_task,
    orchestrator_update_step_one,
    close_temporary_container,
)
app = web.Application()

# DYNAMIC PROMETHEUS TARGETS
app.add_routes([web.get('/targets', handle_targets)])

# SOFTWARE ORCHESTRATION (eg give me a `upipe`)
app.add_routes([web.get('/get', handle_ips_by_label)])

# SHUTDOWN
def handle_signal(app, loop, signame):
    logging.info(f"Received signal {signame}, gracefully shutting down...")
    loop.create_task(app.shutdown())

# AUTOMATIC UPDATE
CLOSE_CONTAINER_ID = os.getenv("CLOSE_CONTAINER_ID", False)
if CLOSE_CONTAINER_ID:
    app.on_startup.append(orchestrator_update_step_one)

if os.getenv("AUTOMATIC_UPDATE", True) != "false"  and not CLOSE_CONTAINER_ID:
    app.on_startup.append(start_update_task)
    logging.info("Will pull images and update out-of-date containers")
else:
    logging.info("Will NOT pull images and update out-of-date containers")

FINAL_CLOSE_CONTAINER_ID = os.getenv("FINAL_CLOSE_CONTAINER_ID", False)
if FINAL_CLOSE_CONTAINER_ID:
    logging.info(f"Will close {FINAL_CLOSE_CONTAINER_ID}")
    app.on_startup.append(close_temporary_container)

# SPOTTERS ORCHESTRATION
SPOTTERS_AMOUNT = os.getenv("SPOTTERS_AMOUNT", 0)
logging.info(f"running {SPOTTERS_AMOUNT} spotters")
if int(SPOTTERS_AMOUNT):
    app.on_startup.append(start_orchestrator)
    app.on_shutdown.append(delete_all_managed_containers)

if __name__ == '__main__':
    logging.info("Starting Orchestrator service on port 8000...")
    web.run_app(app, port=8000, handle_signals=True)
