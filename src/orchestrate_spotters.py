import asyncio, logging, docker, os, datetime
from aiohttp import web

from get_spot_ponderation import get_ponderation

orchestration_label = "network.exorde.orchestration_managed"


def build_container_conciliator():
    online_state_map = {}
    label_filter = {'label': orchestration_label}
    image_prefix = "exordelabs"  # Prefix to convert module names to image names

    async def reconcile_containers(desired_state):
        logging.info("reconcile loop")
        nonlocal online_state_map
        client = docker.from_env()

        # Retrieve all containers with our specific label
        current_containers = client.containers.list(filters=label_filter, all=True)
        logging.info(f"current containers: {current_containers}")

        # Update online_state_map to reflect the current state grouped by module
        online_state_map.clear()
        for container in current_containers:
            module_label_value = container.labels.get(orchestration_label)
            if module_label_value:
                online_state_map.setdefault(module_label_value, []).append(container)
        
        logging.info(f"desired state: {desired_state}")
        for module, desired_count in desired_state.items():
            current_containers = online_state_map.get(module, [])
            current_count = len(current_containers)
            image_name = f"{image_prefix}/spot{module}:latest"  # Convert module to image name
            
            # Spawn missing containers
            if current_count < desired_count:
                for _ in range(desired_count - current_count):
                    try:
                        client.containers.run(image_name, labels={orchestration_label: module}, detach=True)
                        logging.info(f"Spawned new container for module {module} using image {image_name}")
                    except Exception as e:  # Adjusted to catch all exceptions for simplicity
                        logging.error(f"Failed to spawn container for module {module}: {e}")
            
            # Remove excess containers
            elif current_count > desired_count:
                for container in current_containers[desired_count:]:
                    try:
                        container.remove(force=True)
                        logging.info(f"Removed excess container for module {module}")
                    except Exception as e:
                        logging.error(f"Error removing container for module {module}: {e}")

            containers = client.containers.list(filters={'label': orchestration_label, 'ancestor': image_name}, all=True)
            for container in containers:
                logs = container.logs().decode('utf-8')
                status = container.status
                exit_code = container.attrs['State']['ExitCode']
                logging.info(f"Logs for container {container.short_id}: {logs}")
                logging.info(f"Status: {status}, Exit Code: {exit_code}")

    return reconcile_containers
reconcile_containers = build_container_conciliator()

async def get_desired_state():
    logging.info('Getting a new state')
    ponderation = await get_ponderation()
    weights = ponderation.weights
    amount_of_containers = int(os.getenv("ORCHESTRATE", 0))
    
    # Calculate the total weight
    total_weight = sum(weights.values())

    # Calculate the intended number of containers per module, normalized by total weights
    module_containers_intended = {
        module: (weight / total_weight) * amount_of_containers for module, weight in weights.items()
    }

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
    adjusted_module_containers = {"hackbc9419ab11eebe56": 1}

    return adjusted_module_containers


async def orchestration_task():
    refresh_time = int(os.getenv("REFRESH_TIME", "3600"))  # Refresh time in seconds
    last_refresh = datetime.datetime.now()  # Track the last refresh time
    desired_state = await get_desired_state()
    logging.info(f"orchestration start : {desired_state}")

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
    app.loop.create_task(orchestration_task())
