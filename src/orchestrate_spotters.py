import asyncio, logging, docker, os, datetime
from aiohttp import web

from get_spot_ponderation import get_ponderation

orchestration_label = "network.exorde.orchestration_managed"
monitoring_label = "network.exorde.monitor"


def build_container_conciliator():
    image_prefix = "exordelabs"  # Prefix to convert module names to image names

    async def reconcile_containers(desired_state):
        logging.info("reconcile loop")
        client = docker.from_env()

        # Retrieve all containers with our specific label
        label_filter = {'label': orchestration_label}
        current_containers = client.containers.list(filters=label_filter, all=True)
        logging.info(f"current containers: {current_containers}")

        online_state_map = {}
        for container in current_containers:
            module_label_value = container.labels.get(orchestration_label)
            if module_label_value:
                online_state_map.setdefault(module_label_value, []).append(container)

        logging.info(f"desired state: {desired_state}")

        for module, desired_count in desired_state.items():
            # Image name construction for the specific module
            image_name = f"{image_prefix}/spot{module}:latest"

            # Explicitly pull the latest version of the image
            pulled_image = client.images.pull(image_name)
            logging.info(f"Pulled image {image_name} with digest {pulled_image.attrs['RepoDigests']}")

            current_containers = online_state_map.get(module, [])
            current_count = len(current_containers)

            # Identify running containers that need to be replaced with the new version
            containers_with_old_version = [container for container in current_containers if container.image.tags[0] != pulled_image.tags[0]]

            for container in containers_with_old_version:
                try:
                    container.remove(force=True)
                    logging.info(f"Removed container {container.short_id} for module {module} due to new image version")
                    current_count -= 1
                except Exception as e:
                    logging.error(f"Failed to remove container {container.short_id} for module {module}: {e}")

            # Spawn or remove containers based on the desired state
            if current_count < desired_count:
                for _ in range(desired_count - current_count):
                    try:
                        client.containers.run(
                            image_name, labels={
                                orchestration_label: module,
                                monitoring_label: "true"
                            }, network="exorde-network", detach=True
                        )
                        logging.info(f"Spawned new container for module {module} using image {image_name}")
                    except Exception as e:
                        logging.error(f"Failed to spawn container for module {module}: {e}")
            elif current_count > desired_count:
                excess_containers = current_containers[desired_count:]
                for container in excess_containers:
                    try:
                        container.remove(force=True)
                        logging.info(f"Removed excess container {container.short_id} for module {module}")
                    except Exception as e:
                        logging.error(f"Error removing excess container {container.short_id} for module {module}: {e}")
    return reconcile_containers

reconcile_containers = build_container_conciliator()

async def get_desired_state():
    logging.info('Getting a new state')
    ponderation = await get_ponderation()
    weights = ponderation.weights
    amount_of_containers = int(os.getenv("ORCHESTRATE", 0))
    logging.info(f"Amount of containers to manage : {amount_of_containers}")

    # Calculate the total weight
    total_weight = sum(weights.values())

    # Calculate the intended number of containers per module, normalized by total weights
    module_containers_intended = {
        module: (weight / total_weight) * amount_of_containers for module, weight in weights.items()
    }
    logging.info(f"module_containers_intended : {module_containers_intended}")

    # Adjust for rounding issues to ensure the sum of allocated containers matches the amount_of_containers exactly
    # This can be done by distributing rounding errors
    module_containers = module_containers_intended.copy()
    rounded_total = sum(round(count) for count in module_containers.values())
    while rounded_total != amount_of_containers:
        for module in module_containers.keys():
            if rounded_total < amount_of_containers:
                module_containers[module] += 0.1
                rounded_total = sum(round(count) for count in module_containers.values())
                if rounded_total == amount_of_containers:
                    break
            elif rounded_total > amount_of_containers:
                module_containers[module] -= 0.1
                rounded_total = sum(round(count) for count in module_containers.values())
                if rounded_total == amount_of_containers:
                    break

    # Ensure the counts are integers
    adjusted_module_containers = {module: round(count) for module, count in module_containers.items()}
    logging.info(adjusted_module_containers)
    return adjusted_module_containers

async def delete_all_managed_containers(app):
    """
    In order to sanitize the state, we delete every container that are managed
    by the orchestration label
    """
    logging.info("Deleting all containers managed by our label...")
    client = docker.from_env()
    managed_containers = client.containers.list(filters={'label': orchestration_label}, all=True)
    logging.info('Shutting down managed containers')
    for container in managed_containers:
        try:
            container.remove(force=True)
            logging.info(f"Deleted container: {container.short_id}")
        except Exception as e:
            logging.error(f"Failed to delete container: {container.short_id}, Error: {e}")

async def orchestration_task(app):
    refresh_time = int(os.getenv("REFRESH_TIME", "3600"))  # Refresh time in seconds
    last_refresh = datetime.datetime.now()  # Track the last refresh time
    desired_state = await get_desired_state()
    logging.info(f"orchestration start : {desired_state}")
    await delete_all_managed_containers(app)

    while True:
        logging.info("orchestration loop")
        current_time = datetime.datetime.now()
        # Calculate the elapsed time since the last refresh
        elapsed_time = (current_time - last_refresh).total_seconds()

        # Check if it's time to refresh the state
        if elapsed_time >= refresh_time:
            desired_state = await get_desired_state()
            logging.info(f"orchestration loop : {desired_state}")
            last_refresh = datetime.datetime.now()  # Update the last refresh time

        await reconcile_containers(desired_state)
        # Wait for a short interval before checking again
        await asyncio.sleep(60)  # Adjust this sleep time as needed


async def start_orchestrator(app: web.Application):
    app.loop.create_task(orchestration_task(app))
