from typing import Any, Dict, Optional, Union

from datahub.ingestion.graph.client import DataHubGraph


def _parse_response(response: Dict) -> Dict:
    value_str = response["value"].strip("{}")

    args_part = value_str.split(", result=")[0].replace("args=", "")
    result_part = value_str.split(", result=")[1]

    parsed = {"args": _parse_args(args_part), "result": _parse_result(result_part)}
    return parsed


def _parse_dict(content: str) -> Dict:
    content = content.strip("()").strip()
    items = [item.strip() for item in content.split(", ")]

    pairs = []
    for item in items:
        if "=" in item:
            k, v = item.split("=", 1)
            pairs.append((k.strip(), v.strip()))

    return {k: _convert_value(v) for k, v in pairs}


def _parse_args(args_str: str) -> Dict:
    content = args_str.replace("RestoreIndicesArgs(", "").replace(")", "").strip()
    return _parse_dict(content)


def _parse_result(result_str: str) -> Dict:
    content = result_str.replace("RestoreIndicesResult(", "").replace(")", "").strip()
    return _parse_dict(content)


def _convert_value(v: str) -> Any:
    if v == "null":
        return None
    elif v.isdigit():
        return int(v)
    elif v == "[]":
        return []
    elif v == "":
        return ""
    elif v == "false":
        return False
    elif v == "true":
        return True
    elif v.startswith("urn:"):
        return v
    return v


def restore_indices(
    graph: DataHubGraph,
    start: int,
    batch_size: int,
    limit: int,
    urn: Optional[str] = None,
    urn_like: Optional[str] = None,
    aspect: Optional[str] = None,
) -> Dict:
    if urn is None and urn_like is None:
        raise RuntimeError("Either urn or urn_like must be present")

    url = f"{graph.config.server}/operations?action=restoreIndices"
    payload_dict: Dict[str, Union[str, int]] = {
        "start": start,
        "batchSize": batch_size,
        "limit": limit,
    }
    if urn_like is not None:
        payload_dict["urnLike"] = urn_like
    if urn is not None:
        payload_dict["urn"] = urn
    if aspect is not None:
        payload_dict["aspect"] = aspect
    response = graph._post_generic(
        url=url,
        payload_dict=payload_dict,
    )
    return _parse_response(response)
