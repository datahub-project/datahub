from collections import OrderedDict
from typing import Any, Tuple


def _pre_handle_union_with_aliases(
    obj: Any,
    from_pattern: str,
    to_pattern: str,
) -> Tuple[bool, Any]:
    # PDL supports "Unions with aliases", which are unions that have a field name
    # that is different from the type name.
    # See https://linkedin.github.io/rest.li/pdl_schema#union-with-aliases
    # When generating the avro schema, Rest.li adds a "fieldDiscriminator" field
    # to disambiguate between the different union types.

    if "fieldDiscriminator" in obj:
        # On the way out, we need to remove the field discriminator.
        field = obj["fieldDiscriminator"]
        return True, {
            field: _json_transform(obj[field], from_pattern, to_pattern, pre=True)
        }

    return False, None


def _post_handle_unions_with_aliases(
    obj: Any,
    from_pattern: str,
    to_pattern: str,
) -> Tuple[bool, Any]:
    # Note that "CostCostClass" is the only usage of a union with aliases in our
    # current PDL / metadata model, so the below hardcoding works.
    # Because this method is brittle and prone to becoming stale, we validate
    # the aforementioned assumption in our tests.

    if (
        set(obj.keys()) == {"cost", "costType"}
        and isinstance(obj["cost"], dict)
        and len(obj["cost"].keys()) == 1
        and list(obj["cost"].keys())[0] in {"costId", "costCode"}
    ):
        # On the way in, we need to add back the field discriminator.
        return True, {
            "cost": {
                **obj["cost"],
                "fieldDiscriminator": list(obj["cost"].keys())[0],
            },
            "costType": obj["costType"],
        }

    return False, None


def _json_transform(obj: Any, from_pattern: str, to_pattern: str, pre: bool) -> Any:
    if isinstance(obj, (dict, OrderedDict)):
        if len(obj.keys()) == 1:
            key: str = list(obj.keys())[0]
            value = obj[key]
            if key.startswith(from_pattern):
                new_key = key.replace(from_pattern, to_pattern, 1)
                return {
                    new_key: _json_transform(value, from_pattern, to_pattern, pre=pre)
                }

        if pre:
            handled, new_obj = _pre_handle_union_with_aliases(
                obj, from_pattern, to_pattern
            )
            if handled:
                return new_obj

        if not pre:
            handled, new_obj = _post_handle_unions_with_aliases(
                obj, from_pattern, to_pattern
            )
            if handled:
                return new_obj

        new_obj = {
            key: _json_transform(value, from_pattern, to_pattern, pre=pre)
            for key, value in obj.items()
            if value is not None
        }

        return new_obj
    elif isinstance(obj, list):
        new_obj = [
            _json_transform(item, from_pattern, to_pattern, pre=pre) for item in obj
        ]
        return new_obj
    elif isinstance(obj, bytes):
        return obj.decode()
    return obj


def pre_json_transform(obj: Any) -> Any:
    """Usually called before sending avro-serialized json over to the rest.li server"""
    return _json_transform(
        obj,
        from_pattern="com.linkedin.pegasus2avro.",
        to_pattern="com.linkedin.",
        pre=True,
    )


def post_json_transform(obj: Any) -> Any:
    """Usually called after receiving restli-serialized json before instantiating into avro-generated Python classes"""
    return _json_transform(
        obj,
        from_pattern="com.linkedin.",
        to_pattern="com.linkedin.pegasus2avro.",
        pre=False,
    )
