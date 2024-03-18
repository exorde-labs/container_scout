from typing import Union
from aiohttp import ClientSession
from aiodocker import Docker

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
) -> Union[list[str], None]:
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
                return result
            else:
                # Corrected to access 'status' and 'reason' instead of 'error'
                raise Exception(f"HTTP Error {response.status}: {response.reason}")


async def get_local_image_sha(
    image_name: str, tag: str = "latest"
) -> Union[list[str], None]:
    """
    THIS DIGEST IS NOT WHAT YOU THINK IT IS.

    Using docker pull :latest does not store the online digest, therfor the
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


