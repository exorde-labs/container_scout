from typing import Union
import logging, asyncio
from aiohttp import ClientSession
from aiodocker import Docker
from aiohttp import web
import os
import json
from datetime import datetime, timedelta

async def get_docker_hub_token(
    image_name: str
) -> str:
    async with ClientSession() as session:
        url = f"https://auth.docker.io/token?service=registry.docker.io&scope=repository:{image_name}:pull"
        async with session.get(url) as response:
            data = await response.json()
            return data["token"]


async def get_image_manifest(
    image_name: str, tag: str = "latest"
) -> Union[tuple[str, list[str]], None]:
    token = await get_docker_hub_token(image_name)
    headers = {
        "Accept": "application/vnd.docker.distribution.manifest.v2+json",
        "Authorization": f"Bearer {token}"
    }
    url = f"https://registry-1.docker.io/v2/{image_name}/manifests/{tag}"
    async with ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                result: list[str] = []
                for manifest in data['manifests']:
                    if manifest.get('annotations', None):
                        r:str = manifest["annotations"]["vnd.docker.reference.digest"]
                        result.append(r)
                return image_name, result
            else:
                # Corrected to access 'status' and 'reason' instead of 'error'
                raise Exception(f"HTTP Error {response.status}: {response.reason}")


async def get_local_image_sha(
    image_name: str, tag: str = "latest"
) -> Union[list[str], None]:
    """
    DO NOT USE, THIS DIGEST IS NOT WHAT YOU THINK IT IS.

    Using docker pull :latest does not store the online digest, and the
    digest retrieved from this function is NOT the one returned by docker-hub.
    """
    async with Docker() as docker:
        images = await docker.images.list()
        for image in images:
            if image['RepoTags'] is None:  # Handle images without RepoTags
                continue
            if f"{image_name}:{tag}" in image['RepoTags']:
                return image
        return None

async def retrieve_list_of_containers_to_watch(client):
    return await client.containers.list(
        filters={"label": ["network.exorde.monitor=true"]}
    )

async def get_image_from_container(container):
    details = await container.show()
    return container, details['Config']['Image']

async def images_of_containers(containers):
    r = await asyncio.gather(*[get_image_from_container(container) for container in containers])
    return list(set(r))

async def get_digests_for_imgs(imgs: list[str]):
    digests = await asyncio.gather(*[get_image_manifest(image) for __container__, image in imgs])
    return { img: digest for (img, digest) in digests }


def build_update_function(delay: int, validity_threshold_seconds: int):
    """Builds and returns an asynchronous function to update containers with specified delay and validity threshold for image pulls."""
    images_to_update = {}
    last_pull_times = {}

    async def pull_image_if_needed(docker, image):
        """Pulls a Docker image if it hasn't been pulled recently."""
        now = datetime.now()
        if image not in last_pull_times or now - last_pull_times[image] > timedelta(seconds=validity_threshold_seconds):
            print(f"Pulling image {image}...")
            await docker.images.pull(image)
            print(f"Image {image} pulled.")
            last_pull_times[image] = now
        else:
            print(f"Image {image} pull skipped due to recent pull.")

    async def recreate_container(docker, container, image):
        """Recreates a container asynchronously."""
        container_id = container.id  # Adjusted to access the container's ID property
        print(f"Recreating container {container_id} with image {image}...")
        await container.stop()
        await container.delete()
        print(f"Container {container_id} stopped and deleted.")

    async def schedule_update(container, image: str):
        """Schedules an update for a container, ensuring the image is pulled only once within the validity window."""
        if image not in images_to_update:
            images_to_update[image] = []
        images_to_update[image].append(container)

        # Pull and recreate immediately within the context of scheduling to honor delay and validity
        async with Docker() as docker:
            await pull_image_if_needed(docker, image)
            await asyncio.sleep(delay)  # Delay before recreating the container
            await recreate_container(docker, container, image)

    return schedule_update
# Example usage:
schedule_update = build_update_function(delay=5, validity_threshold_seconds=30)  # Set delay to 5 seconds and validity threshold to 30 seconds


def build_updater():
    module_digest_map = {} # stores the digests
    async def enforce_versioning(client):
        """
        1. retrieve the latest digest for each currently running container (module_digest_map)
        2. if a digest is different in current module_digest_map (or empty) -> trigger update for that container
        3. update module_digest_map
        """
        nonlocal module_digest_map
        containers_to_watch = await retrieve_list_of_containers_to_watch(client)
        containers_and_images = await images_of_containers(containers_to_watch)
        images = [img for img in containers_and_images]
        latest_digests = await get_digests_for_imgs(images)

        for container, img in containers_and_images:
            latest_digest = latest_digests[img]
            current_digest = module_digest_map.get(container, None)

            # If digest is different or container is not in module_digest_map, trigger update
            if current_digest is None or current_digest != latest_digest:
                await schedule_update(container, img)
                # Update the module_digest_map with the latest digest
                module_digest_map[container] = latest_digest

    return enforce_versioning
enforce_versioning = build_updater()

async def update_task(app):
    """
    For each container with `network.exorde.monitor=true` we retrieve their 
    image name and monitor the version by pulling the docker-hub digest
    periodicaly and triggering an update as soon as a new digest is retrieved.

    note: a weakness of this algorith is that it will not be able to determin
    if an image is out-of-date ; launching an out-of-date client would not be
    managed by this procedure.

    It is therfor required to trigger an update on first-run.
    """
    client = Docker()        
    while True:
        logging.info("Update task")

        await enforce_versioning(client)
        await asyncio.sleep(
            int(os.getenv("UPDATE_REFRESH_TIME", 5*60))
        ) # default to 5min (same as watchtower)


async def start_update_task(app: web.Application):
    app.loop.create_task(update_task(app))
