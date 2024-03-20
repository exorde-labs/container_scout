import asyncio, logging, os, datetime
import docker
from aiodocker import Docker
from aiohttp import web
from get_spot_ponderation import get_ponderation

orchestration_label = "network.exorde.orchestrate"
monitoring_label = "network.exorde.monitor"


def build_container_conciliator():
    image_prefix = "exordelabs"  # Prefix to convert module names to image names
    async def reconcile_containers(desired_state):
        client = Docker()
        logging.info("reconcile loop")
        
        # Fetch all managed containers
        containers = await client.containers.list(
            filters={"label": ["network.exorde.orchestrate=spotter"]}
        )

        logging.info(f"desired_state: {desired_state}")
        logging.info(f"found {len(containers)} containers")
        
        # Organize containers by their image prefix
        current_state = {}
        for container in containers:
            container_details = await container.show()
            image_name = container_details['Config']['Image']
            base_image_name = image_name.replace(f"{image_prefix}/spot", "")
            
            if base_image_name in current_state:
                current_state[base_image_name].append(container)
            else:
                current_state[base_image_name] = [container]
        
        # Reconcile containers
        for image, desired_count in desired_state.items():
            prefixed_image = f"{image_prefix}/spot{image}"  # Apply prefix to image name
            current_containers = current_state.get(image, [])
            current_count = len(current_containers)
            
            if current_count < desired_count:
                # Start new containers
                for _ in range(desired_count - current_count):
                    logging.info(f"Starting new container for image {prefixed_image}...")
                    container = await client.containers.create_or_replace(
                        config={"Image": prefixed_image, "Labels": {
                            "network.exorde.orchestrate": "spotter",
                            "network.exorde.monitor": "true"
                        }},
                        name=f"{image_prefix}_{image}_{current_count + _}"
                    )
                    await container.start()
            elif current_count > desired_count:
                # Stop and remove extra containers
                extra_containers = current_containers[desired_count:]
                for container in extra_containers:
                    logging.info(f"Stopping and removing container {container.id} for image {prefixed_image}...")
                    await container.stop()
                    await container.delete()

        await client.close()
    return reconcile_containers
reconcile_containers = build_container_conciliator()


async def get_desired_state() -> dict[str, int]:
    logging.info('Getting a new state')
    ponderation = await get_ponderation()
    weights = ponderation.weights
    amount_of_containers = int(os.getenv("SPOTTERS_AMOUNT", 0))
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
    adjusted_module_containers = {
        module: round(count) for module, count in module_containers.items()
    }
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
        await asyncio.sleep(5)  # Adjust this sleep time as needed


async def start_orchestrator(app: web.Application):
    app.loop.create_task(orchestration_task(app))
