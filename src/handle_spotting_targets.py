"""Endpoint is used by `spotters` to determin their `upipe` target"""
import logging, docker
from aiohttp import web

async def fetch_ips_by_label(filter_key, filter_value):
    logging.info("Fetching IPs by label...")
    client = docker.from_env()
    ips = []
    for container in client.containers.list():
        container_labels = container.labels
        logging.info(f"Looking at {container} with {container_labels}")
        if filter_key and filter_value and container_labels.get(filter_key) == filter_value:
            networks = container.attrs['NetworkSettings']['Networks']
            if networks:
                for network_details in networks.values():
                    if 'IPAddress' in network_details and network_details['IPAddress']:
                        ips.append(f"http://{network_details['IPAddress']}:8000")
                        break  # Assuming you want just one IP per container
    logging.info("IPs fetched successfully.")
    return ips

async def handle_ips_by_label(request):
    # Extract the filter from the query parameters
    query_params = request.query
    if query_params:
        filter_key, filter_value = list(query_params.items())[0]  # Assumes a single filter key-value pair
        logging.info(f"Filter_key is = {filter_key}, filter_value is = {filter_value}")
        ips = await fetch_ips_by_label(filter_key, filter_value)
        return web.json_response(ips)
    else:
        return web.json_response({"error": "No filter provided"}, status=400)
