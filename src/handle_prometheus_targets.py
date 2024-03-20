"""Endpoint is used by prometheus to dynamicly fetch it's scraping targets"""
import os, logging, docker
from aiohttp import web

async def fetch_docker_targets():
    logging.info("Fetching Docker targets...")
    # Fetch and parse the PROMETHEUS_TARGET_DISCOVERY_LABEL environment variable
    filter_env = os.getenv('PROMETHEUS_TARGET_DISCOVERY_LABEL', '')  # Example format: "com.example.department:IT"
    filter_key, filter_value = filter_env.split(':', 1) if ':' in filter_env else (None, None)

    client = docker.from_env()
    targets = []
    for container in client.containers.list():
        container_labels = container.labels  # Fetch labels for filtering
        # Skip containers that don't match the filter criteria, if any
        if filter_key and filter_value and not container_labels.get(
            filter_key
        ) == filter_value:
            continue  # Skip this container as it doesn't match the filter

        networks = container.attrs['NetworkSettings']['Networks']
        ip = None
        for network_name, network_details in networks.items():
            if 'IPAddress' in network_details and network_details['IPAddress']:
                ip = network_details['IPAddress']
                break  # Break after finding the first network with an IP

        if ip:
            port = "8000"  # Adjust with your actual port
            # Only include the job label here, as per requirements
            prometheus_labels = {"__meta_prometheus_job": container.labels.get(
                "com.docker.compose.service", "unknown_service")
            }
            # Format the target as specified
            targets.append({
                "labels": prometheus_labels, "targets": [f"{ip}:{port}"]
            })
        else:
            logging.warning(f"No IP found for container {container.id}")

    logging.info("Targets fetched successfully.")
    return targets

async def handle_targets(request):
    targets = await fetch_docker_targets()  # Your existing function to fetch targets
    response = web.json_response(targets)
    response.headers['Content-Type'] = 'application/json'  # Ensure correct Content-Type
    return response
