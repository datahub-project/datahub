import json
import logging
from typing import List

from datahub.configuration.config_loader import EnvResolver
from datahub.secret.secret_store import SecretStore

logger = logging.getLogger(__name__)


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
                    # HACK: We previously, incorrectly replaced newline characters with
                    # a r'\n' string. This was a lossy conversion, since we can no longer
                    # distinguish between a newline character and the literal '\n' in
                    # the secret value. For now, we assume that all r'\n' strings are
                    # actually newline characters. This will break if a secret value
                    # genuinely contains the string r'\n'.
                    # Once this PR https://github.com/datahub-project/datahub/pull/9484
                    # has baked for a while, we should be able to remove this hack.
                    # TODO: This logic should live in the DataHub secret client/store,
                    # not the general secret resolution logic.
                    secret_value = secret_value.replace(r"\n", "\n")

                    final_secret_values[secret_name] = secret_value
        except Exception:
            logger.exception(
                f"Failed to fetch secret values from secret store with id {secret_store.get_id()}"
            )
    return final_secret_values


def resolve_recipe(
    recipe: str, secret_stores: List[SecretStore], strict_env_syntax: bool = True
) -> dict:
    # Note: the default for `strict_env_syntax` is normally False, but here we override
    # it to be true. Particularly when fetching secrets from external secret stores, we
    # want to be more careful about not over-fetching secrets.

    json_recipe_raw = json.loads(recipe)

    # 1. Extract all secrets needing resolved.
    secrets_to_resolve = EnvResolver.list_referenced_variables(
        json_recipe_raw, strict_env_syntax=strict_env_syntax
    )

    # 2. Resolve secret values
    secret_values_dict = resolve_secrets(list(secrets_to_resolve), secret_stores)

    # 3. Substitute secrets into recipe file
    resolver = EnvResolver(
        environ=secret_values_dict, strict_env_syntax=strict_env_syntax
    )
    json_recipe_resolved = resolver.resolve(json_recipe_raw)

    return json_recipe_resolved
