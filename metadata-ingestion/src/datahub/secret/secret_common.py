import json
import logging
import re
from typing import List

from datahub.secret.secret_store import SecretStore

logger = logging.getLogger(__name__)

"""
The following was borrowed from the acryl-executor repository.
"""


def resolve_secrets(secret_names: List[str], secret_stores: List[SecretStore]) -> dict:
    # Attempt to resolve secret using by checking each configured secret store.
    final_secret_values = dict({})

    for secret_store in secret_stores:
        try:
            # Retrieve secret values from the store.
            secret_values_dict = secret_store.get_secret_values(secret_names)
            # Overlay secret values from each store, if not None.
            for secret_name, secret_value in secret_values_dict.items():
                if secret_value is not None:
                    final_secret_values[secret_name] = secret_value
        except Exception:
            logger.exception(
                f"Failed to fetch secret values from secret store with id {secret_store.get_id()}"
            )
    return final_secret_values


def resolve_recipe(recipe: str, secret_stores: List[SecretStore]) -> dict:
    # Now attempt to find and replace all secrets inside the recipe.
    secret_pattern = re.compile(".*?\\${(\\w+)}.*?")

    resolved_recipe = recipe
    secret_matches = secret_pattern.findall(resolved_recipe)

    # 1. Extract all secrets needing resolved.
    secrets_to_resolve = []
    if secret_matches:
        for match in secret_matches:
            secrets_to_resolve.append(match)

    # 2. Resolve secret values
    secret_values_dict = resolve_secrets(secrets_to_resolve, secret_stores)

    # 3. Substitute secrets into recipe file
    if secret_matches:
        for match in secret_matches:
            # a. Check if secret was successfully resolved.
            secret_value = secret_values_dict.get(match)
            if secret_value is None:
                # Failed to resolve secret.
                raise Exception(
                    f"Failed to resolve secret with name {match}. Aborting recipe execution."
                )

            # b. Substitute secret value.
            resolved_recipe = resolved_recipe.replace(f"${{{match}}}", secret_value)

    json_recipe = json.loads(resolved_recipe, strict=False)
    return json_recipe
