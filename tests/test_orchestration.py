import logging
import pytest
import asyncio
from orchestrate_spotters import get_desired_state, reconcile_containers
from aiodocker import Docker

@pytest.mark.asyncio
async def test_get_desired_state():
    desired_state = await get_desired_state()
    assert isinstance(desired_state, dict)


@pytest.mark.asyncio
async def test_reconcile_containers_creation():
    desired_state = { "ap98j3envoubi3fco1kc": 1 }
    await reconcile_containers(desired_state)
    await asyncio.sleep(2)
    
    docker = Docker()
    containers = await docker.containers.list(all=True)
    actual_state = {}
    for container in containers:
        image = container['Image']
        if image.startswith('exordelabs/spot'):
            base_image = image.replace('exordelabs/spot', '')
            if base_image in actual_state:
                actual_state[base_image] += 1
            else:
                actual_state[base_image] = 1
    await docker.close()

    # Assertion to ensure actual_state matches the desired_state, including handling for desired count of 0
    for image, count in desired_state.items():
        if count == 0:
            assert image not in actual_state or actual_state[image] == 0, f"Image {image} should have 0 containers, found {actual_state.get(image, 0)}."
        else:
            assert image in actual_state, f"Image {image} is missing in actual state."
            assert actual_state[image] == count, f"Image {image} has {actual_state[image]} containers, expected {count}."

@pytest.mark.asyncio
async def test_reconcile_containers_closure():
    desired_state = { "ap98j3envoubi3fco1kc": 0 }
    await reconcile_containers(desired_state)
    await asyncio.sleep(2)
    docker = Docker()
    containers = await docker.containers.list(all=True)
    actual_state = {}
    for container in containers:
        image = container['Image']
        if image.startswith('exordelabs/spot'):
            base_image = image.replace('exordelabs/spot', '')
            if base_image in actual_state:
                actual_state[base_image] += 1
            else:
                actual_state[base_image] = 1
    await docker.close()
    # Assertion to ensure actual_state matches the desired_state, including handling for desired count of 0
    for image, count in desired_state.items():
        if count == 0:
            assert image not in actual_state or actual_state[image] == 0, f"Image {image} should have 0 containers, found {actual_state.get(image, 0)}."
        else:
            assert image in actual_state, f"Image {image} is missing in actual state."
            assert actual_state[image] == count, f"Image {image} has {actual_state[image]} containers, expected {count}."
