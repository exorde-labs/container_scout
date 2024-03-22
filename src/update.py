from typing import Union
import logging, asyncio
from aiohttp import ClientSession
from aiodocker import Docker
from aiohttp import web
import os
import json
from datetime import datetime

# Custom serializer for datetime objects
def datetime_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

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
    # Step 1: Separate any tag included in the image name
    if ':' in image_name:
        image_name = image_name.split(':')[0]

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
                for manifest in data.get('manifests', []):
                    if manifest.get('annotations', None):
                        r: str = manifest["annotations"]["vnd.docker.reference.digest"]
                        result.append(r)
                return image_name, result
            else:
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
    """
    This also sets the /orchestrator image last to allow the full update of every
    container BEFORE triggering self update
    """
    # Retrieve container/image pairs asynchronously
    r = await asyncio.gather(
        *[get_image_from_container(container) for container in containers]
    )
    
    # Separate pairs with "exordelabs/orchestrator" image from others
    orchestrator_pairs = []
    other_pairs = []
    for pair in r:
        if pair[1] == "exordelabs/orchestrator":
            orchestrator_pairs.append(pair)
        else:
            other_pairs.append(pair)
    
    # Reorder the list to place "exordelabs/orchestrator" pairs last
    reordered_pairs = other_pairs + orchestrator_pairs
    
    return reordered_pairs

async def get_digests_for_imgs(imgs: list[str]):
    async def safe_get_image_manifest(image):
        logging.info(f"Getting digest for {image}")
        try:
            result = await get_image_manifest(image)
            return image, result
        except Exception as e:
            logging.exception(f"getting manifest for '{image}': {e}")
            return image, None

    results = await asyncio.gather(
        *[safe_get_image_manifest(image) for __container__, image in imgs]
    )
    
    # Filter out None results
    filtered_results = filter(lambda x: x[1] is not None, results)
    
    return {img: digest for img, digest in filtered_results}


async def close_temporary_container(app):
    """
    STEP 2 OF ORCHESTRATOR UPDATE
    (this runs inside the new orchestrator container)
    """
    FINAL_CLOSE_CONTAINER_ID = os.getenv("FINAL_CLOSE_CONTAINER_ID")
    logging.info(f"Running `close_temporary_container` procedure on {FINAL_CLOSE_CONTAINER_ID}")
    docker = Docker()
    try:
        existing_container = await docker.containers.get(FINAL_CLOSE_CONTAINER_ID)
        await existing_container.stop()
        await existing_container.delete(force=True)
        logging.info("Cleaned up temporary container")
    except:
        logging.info("Could not find the container")
    await docker.close()

async def orchestrator_update_step_one(app):
    """
    STEP 1 OF ORCHESTRATOR UPDATE
    (this runs inside a container called orchestrator-*-temp)

        a) retrieve self container_id
            this is done using an additional http endpoint (see handle_container_id)

        b) retrieve the old container

        c) get details from old_container

        d) create new container with same details
            + FINAL_CLOSE_CONTAINER_ID
            which is the the container_id of the `self`

        e) stop and delete old container
    """
    logging.info(
        "I'm a temporary container ; Running `orchestrator_update_step_one` procedure"
    )
    CLOSE_CONTAINER_ID = os.getenv("CLOSE_CONTAINER_ID")
    docker = Docker()
    existing_container = await docker.containers.get(CLOSE_CONTAINER_ID)

    new_container_host_config = json.loads(
        os.getenv("NEW_CONTAINER_HOST_CONFIG", "{}")
    )
    details = await existing_container.show()
    config = dict(details['Config'])
    config['HostConfig'] = new_container_host_config
    config['Env'].append(f"LAST_PULL_TIMES={os.getenv('LAST_PULL_TIMES')}")
    config['Env'].append(f"MODULE_DIGEST_MAP={os.getenv('MODULE_DIGEST_MAP')}")
    
    """
     -> we cannot know `container_id` before the creation so we cannot get it 
        trough `env`
     -> can we get it by `calculation` ?
        -> containers.list().filter(name='orchestrator-*-temp')[0].id
    """
    containers = await docker.containers.list(filters={
        "label": ["network.exorde.monitor=true"]
    })

    self_id = ''
    for container in containers:
        logging.info(f"Looking at {container.id}")
        container_info = await container.show()
        name = container_info.get("Name", "")
        logging.info(f"\t...name is '{name}'")
        if "orchestrator" in name and "temp" in name:
            logging.info(f"Found container id !")
            self_id = container.id
    assert self_id != ''
    logging.info(f"Found self container id : I'm {self_id} !")
    # this is identifying SELF (or the temp container)
    config['Env'].append(f"FINAL_CLOSE_CONTAINER_ID={self_id}")
    logging.info(f"Creating new container")
    logging.info("Here's it's env")
    logging.info(json.dumps(config['Env'], indent=4))
    new_container = await docker.containers.create_or_replace(
        name=details["Name"][1:].replace("-temp", ""),
        config=config
    )
    await new_container.start()
    
    try:
        await existing_container.stop()
        await existing_container.delete(force=True)
    except:
        logging.exception("Could not stop and delete the container")

    await docker.close()
    logging.info(
        f"New version at {new_container.id} started, it will take over, bye !"
    )

async def update_orchestrator(
    original_container, details, module_digest_map, last_pull_times
):
    """
    The dificulty is that closing the orchestrator process would close this 
    running thread.

    A solution is to create the new container first and then destroy the old one.

    We can pass,

        - `module_digest_map` (MODULE_DIGEST_MAP), 

        - `last_pull_times` (LAST_PULL_TIMES) 

        - the `container_id` (CLOSE_CONTAINER_ID)

    to the newly created container.

    Which allows the new orchestrator to sync on the job,

        - delete the parent properly 
        (in-proc closing prevents delete, and delete requires closes).

       - continue on the same "phase"

    It's a hassle to use the container_id in order to broadcast a selfupdate
    procedure to N orchestrators. Running multiple orchestrator has no benefit
    and this is therfor not supported. Doing this would result in clones and
    conflicting orch.
   

    We CANNOT pass
        - docker meta-data at creation time
            (eg newly created container_id, since it's known after creation)

    Which forces us to include a network exchange between the two containers,
    this is not a thing on the first container because we already have an
    instance of the container and we have the limitation of only one 
    "orchestrator" instance.

    However, as soon as we have spawned the temporary orchestrator, we cannot
    determin the container_id with exactitude, hence this problem.

    """
    logging.info("Updating the Orchestrator")
    docker = Docker()
    original_configuration = details["Config"]
    
    new_configuration = dict(original_configuration)
    new_configuration['HostConfig'] = details['HostConfig']
    new_configuration['HostConfig']['PortBindings'] = {} # ports are unique resources
    new_configuration['Env'].append(
        f"MODULE_DIGEST_MAP={json.dumps(module_digest_map)}"
    )
    new_configuration['Env'].append(
        f"LAST_PULL_TIMES={json.dumps(last_pull_times, default=datetime_serializer)}"
    )
    new_configuration['Env'].append(
        f"CLOSE_CONTAINER_ID={original_container.id}"
    )
    new_configuration['Env'].append(
        f"NEW_CONTAINER_HOST_CONFIG={json.dumps(details['HostConfig'])}"
    )

    logging.info(
        f"Temporary orchestrator configuration is : \n{json.dumps(new_configuration)}"
    )
    new_container = await docker.containers.create_or_replace(
        name=f"{details['Name'][1:]}-temp",
        config=new_configuration
    )
    await new_container.start()
    logging.info(
        f"Temporary Container {new_container.id} started, it will take over, bye !"
    )
    

async def recreate_container(
    docker, container, module_digest_map, last_pull_times
):
    """Recreates a container asynchronously."""
    details = await container.show()
    config = details["Config"]
    logging.info(f"Recreating container `{container.id}` ({config['Image']})")
    if "exordelabs/orchestrator" in config['Image']:
        # *IMPORTANT* -> ORCHESTRATOR HAS TO BE UPDATED LAST
        await update_orchestrator(
            container, details, module_digest_map, last_pull_times 
        )
        return
    # logging.info(f"{json.dumps(details, indent=4)}")
    config['HostConfig'] = details['HostConfig']
    new_container = await docker.containers.create_or_replace(
        name=details['Name'][1:], config=config
    )
    await new_container.start()

# Custom deserializer for datetime objects
def datetime_deserializer(dict_):
    for key, value in dict_.items():
        try:
            dict_[key] = datetime.fromisoformat(value)
        except (ValueError, TypeError):
            pass  # Not a datetime string, ignore
    return dict_

def build_update_function(delay: int, validity_threshold_seconds: int):
    """
    Builds and returns an asynchronous function that updates containers.
    For each unique image, a single pull operation is performed if needed,
    followed by updates to all containers using that image.
    """
    images_to_update = {}
    last_pull_times = json.loads(
        os.getenv('LAST_PULL_TIMES', '{}'),
        object_hook=datetime_deserializer
    )
    logging.info(f"LAST_PULL_TIMES IS  {json.dumps(last_pull_times, indent=4, default=datetime_serializer)}")

    async def pull_image_if_needed(docker, image):
        """Pulls a Docker image if it hasn't been pulled recently."""
        now = datetime.now()
        if image not in last_pull_times or (now - last_pull_times[image]).total_seconds() > validity_threshold_seconds:
            logging.info(f"Pulling image {image}...")
            await docker.images.pull(image)  
            logging.info(f"Image {image} pulled.")
            last_pull_times[image] = now
            return True
        logging.info(f"Image {image} pull skipped due to recent pull.")
        return False

    async def update_containers(docker, image, containers, module_digest_map):
        """Updates all containers for a given image."""
        for container in containers:
            # Delay before updating the next container to avoid simultaneous downtime
            await asyncio.sleep(delay)
            await recreate_container(docker, container, module_digest_map, last_pull_times)

    async def handle_image_update(image, module_digest_map):
        """Handles updating of containers for a specific image."""
        nonlocal images_to_update
        async with Docker() as docker:
            await asyncio.sleep(5)
            containers = images_to_update[image]
            logging.info(f"Running handle_image_update for {image}")
            logging.info(f"\t - containers are: {[container.id for container in containers]}")
            pulled = await pull_image_if_needed(docker, image)
            if pulled:
                logging.info(
                    f"\t - Image have been pulled, triggering update for {[container.id for container in containers]}"
                )
                await update_containers(
                    docker, image, containers, module_digest_map
                )

    async def schedule_update(container, image: str, module_digest_map):
        """
        Schedules an update for a container, ensuring that for each unique image,
        the pull operation is performed only once, followed by updates to all containers
        using that image.
        """
        if image not in images_to_update:
            images_to_update[image] = [container]
        else:
            images_to_update[image].append(container)

        # Check if this is the first container for the image to schedule the task
        if len(images_to_update[image]) == 1:
            asyncio.create_task(
                handle_image_update(
                    image, module_digest_map
                )
            )

    return schedule_update
schedule_update = build_update_function(
    delay=5, validity_threshold_seconds=30
)

def build_updater():                                                            
    preloaded_module_digest_map = os.getenv('MODULE_DIGEST_MAP', '')            
    if preloaded_module_digest_map != '':                                       
        module_digest_map = json.loads(preloaded_module_digest_map)             
    else:                                                                       
        module_digest_map = {}                                                  
    async def enforce_versioning(client):                                        
        nonlocal module_digest_map 

        logging.info("Enforcing versioning") 
        containers_to_watch = await retrieve_list_of_containers_to_watch(client)
        containers_and_images = await images_of_containers(containers_to_watch) 
        images = [img for img in containers_and_images] 
        logging.info(f"Looking at {len(images)} images") 
        latest_digests = await get_digests_for_imgs(images) 
        for container, img in containers_and_images:
            logging.info(f"\t- checking {container.id}")
            latest_digest = latest_digests[img] 
            current_digest = module_digest_map.get(img, None)
            logging.info(
                f"\t\t - Latest digest for {img}: `{json.dumps(latest_digest, indent=4)}`"
            )
            logging.info(
                f"\t\t - Previous digest for {img}: `{json.dumps(current_digest, indent=4)}`"
            )
            if current_digest is None or current_digest != latest_digest:
                logging.info(f"\t - DETECTED DIGEST CHANGE FOR {img}")
                module_digest_map[img] = latest_digest
                logging.info(f"\t\t - Scheduling an update for {img}")
                await schedule_update(container, img, module_digest_map)

        logging.info("Versioning loop complete") 
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
    await asyncio.sleep(30) # to let other container start
    app.loop.create_task(update_task(app))
