from collections import OrderedDict
from typing import Any


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

        # Avro uses "fieldDiscriminator" for unions between primitive types, while
        # rest.li simply uses the field names. We have to add special handling for this.
        if pre and "fieldDiscriminator" in obj:
            # On the way out, we need to remove the field discriminator.
            field = obj["fieldDiscriminator"]
            return {
                field: _json_transform(obj[field], from_pattern, to_pattern, pre=pre)
            }
        if (
            not pre
            and set(obj.keys()) == {"cost", "costType"}
            and isinstance(obj["cost"], dict)
            and len(obj["cost"].keys()) == 1
            and list(obj["cost"].keys())[0] in {"costId", "costCode"}
        ):
            # On the way in, we need to add back the field discriminator.
            # Note that "CostCostClass" is the only usage of fieldDiscriminator in our models,
            # so this works ok.
            return {
                "cost": {
                    **obj["cost"],
                    "fieldDiscriminator": list(obj["cost"].keys())[0],
                },
                "costType": obj["costType"],
            }

        new_obj: Any = {
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
