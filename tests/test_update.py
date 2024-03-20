
import logging
from aiodocker import Docker
import json, logging, pytest
from update import (
    get_image_manifest, 
    get_docker_hub_token,
    retrieve_list_of_containers_to_watch,
    images_of_containers,
    get_digests_for_imgs,
    enforce_versioning
)


@pytest.mark.asyncio
async def test_get_docker_hub_token():
    token = await get_docker_hub_token("exordelabs/orchestrator")
    logging.info(token)


@pytest.mark.asyncio
async def test_get_image_manifest():
    image, manifests = await get_image_manifest("exordelabs/orchestrator")
    logging.info(f" {image} -> {json.dumps(manifests, indent=4)}")
    image, manifests = await get_image_manifest("exordelabs/transactioneer")
    logging.info(f" {image} -> {json.dumps(manifests, indent=4)}")
    image, manifests = await get_image_manifest("exordelabs/upipe")
    logging.info(f" {image} -> {json.dumps(manifests, indent=4)}")
    image, manifests = await get_image_manifest("exordelabs/bpipe")
    logging.info(f" {image} -> {json.dumps(manifests, indent=4)}")


@pytest.mark.asyncio
async def test_update():
    client = Docker()
    image = "exordelabs/orchestrator"
    container = await client.containers.create_or_replace(
        name="container-for-exordelabs-test",
        config={
        "Image": image, "Labels": {
            "network.exorde.monitor": "true"
        }
    })
    container_b = await client.containers.create_or_replace(
        name="container-for-exordelabs-test-b",
        config={
        "Image": image, "Labels": {
            "network.exorde.monitor": "true"
        }
    })
    await container.start()
    await container_b.start()

    containers_to_watch = await retrieve_list_of_containers_to_watch(client)
    assert len(containers_to_watch) == 2
    imgs = await images_of_containers(containers_to_watch)
    assert len(imgs) == 2 # include container and img

    digests = await get_digests_for_imgs(imgs)
    logging.info(digests)

    await enforce_versioning(client)

    # enforce_versioning should kill the online containers after a pull
    containers_to_watch = await retrieve_list_of_containers_to_watch(client)
    assert len(containers_to_watch) == 0
