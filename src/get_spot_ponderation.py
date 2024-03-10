"""This is used to retrieve the online prefered configuration for scrapers"""

import aiohttp, logging, json
from datetime import datetime, timedelta
from typing import Callable

PONDERATION_URL: str = "https://raw.githubusercontent.com/exorde-labs/TestnetProtocol/main/targets/modules_configuration_v2.json"

from typing import Union, Dict, List
from dataclasses import dataclass

@dataclass
class Ponderation:
    enabled_modules: Dict[str, List[str]]
    generic_modules_parameters: Dict[str, Union[int, str, bool]]
    specific_modules_parameters: Dict[str, Dict[str, Union[int, str, bool]]]
    weights: Dict[str, float]

async def _get_ponderation() -> Ponderation:
    async with aiohttp.ClientSession() as session:
        async with session.get(PONDERATION_URL) as response:
            response.raise_for_status()
            raw_data: str = await response.text()
            try:
                json_data = json.loads(raw_data)
            except Exception as error:
                logging.error(raw_data)
                raise error
            enabled_modules = json_data["enabled_modules"]
            generic_modules_parameters = json_data[
                "generic_modules_parameters"
            ]
            specific_modules_parameters = json_data[
                "specific_modules_parameters"
            ]
            weights = json_data["weights"]
            return Ponderation(
                enabled_modules=enabled_modules,
                generic_modules_parameters=generic_modules_parameters,
                specific_modules_parameters=specific_modules_parameters,
                weights=weights,
            )


def ponderation_geter() -> Callable:
    memoised = None
    last_call = datetime.now()

    async def get_ponderation_wrapper() -> Ponderation:
        nonlocal memoised, last_call
        now = datetime.now()
        if not memoised or (now - last_call) > timedelta(minutes=1):
            last_call = datetime.now()
            memoised = await _get_ponderation()
        return memoised

    return get_ponderation_wrapper


get_ponderation: Callable = ponderation_geter()
