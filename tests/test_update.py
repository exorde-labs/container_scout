
import json
import logging
import pytest
from update import (
    get_image_manifest, 
    get_docker_hub_token, 
)

@pytest.mark.asyncio
async def test_get_docker_hub_token():
    token = await get_docker_hub_token("exordelabs/orchestrator")
    logging.info(token)

@pytest.mark.asyncio
async def test_get_image_manifest():
    data = await get_image_manifest("exordelabs/orchestrator")
    logging.info(json.dumps(data, indent=4))
