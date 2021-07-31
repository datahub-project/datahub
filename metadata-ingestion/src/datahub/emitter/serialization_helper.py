from collections import OrderedDict
from typing import Any


def pre_json_transform(obj: Any) -> Any:
    if isinstance(obj, (dict, OrderedDict)):
        if len(obj.keys()) == 1:
            key = list(obj.keys())[0]
            value = obj[key]
            if key.find("com.linkedin.pegasus2avro.") >= 0:
                new_key = key.replace("com.linkedin.pegasus2avro.", "com.linkedin.")
                return {new_key: pre_json_transform(value)}

        if "fieldDiscriminator" in obj:
            # Field discriminators are used for unions between primitive types.
            field = obj["fieldDiscriminator"]
            return {field: pre_json_transform(obj[field])}

        new_obj: Any = {}
        for key, value in obj.items():
            if value is not None:
                new_obj[key] = pre_json_transform(value)
        return new_obj
    elif isinstance(obj, list):
        new_obj = [pre_json_transform(item) for item in obj]
        return new_obj
    elif isinstance(obj, bytes):
        return obj.decode()
    return obj
