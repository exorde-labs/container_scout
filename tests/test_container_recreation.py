import pytest
import json
import logging
from aiodocker import Docker
from update import recreate_container
import asyncio


@pytest.mark.asyncio
async def test_get_container_configuration():
    client = Docker()
    container = await client.containers.create_or_replace(
        name="test_container",
        config={
            "Image": "exordelabs/orchestrator",
            "Labels": {
                "network.exorde.test": "true"
            }
        }
    )
    await container.start()

    info = await container.show()
    config = info["Config"]

    logging.info(f"{json.dumps(config, indent=4)}")

    await container.stop()
    await container.delete()
    await client.close()

@pytest.fixture
def container_cleanup():
    yield
    async def run():
        logging.info("Removing dangling test containers")
        client = Docker()
        containers = await client.containers.list(
            filters={'label': ["network.exorde.test=true"] }, all=True
        )
        for container in containers:
            await container.stop()
            await container.delete()
        await client.close()
    asyncio.run(run())

@pytest.mark.asyncio
async def test_container_recreation(container_cleanup):
    client = Docker()
    container = await client.containers.create_or_replace(
        name="test_container",
        config={
            "Image": "exordelabs/transactioneer",
            "Labels": {
                "network.exorde.test": "true"
            }
        }
    )
    await container.start()

    containers = await client.containers.list(
        filters={'label': ["network.exorde.test=true"] }, all=True
    )
    assert len(containers) == 1
    await recreate_container(client, container)

    containers = await client.containers.list(
        filters={'label': ["network.exorde.test=true"] }, all=True
    )
    assert len(containers) == 1
    await client.close()
