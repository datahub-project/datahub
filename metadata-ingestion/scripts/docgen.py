import logging
import os
import textwrap
from typing import Any, Dict, List, Optional

import click
from pydantic.dataclasses import dataclass

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.source import Source

logger = logging.getLogger(__name__)


@dataclass
class FieldRow:
    path: str
    type_name: str
    format: str
    default: str
    description: str


def get_definition_dict_from_definition(
    definitions_dict: Dict[str, Any], definition_name: str
) -> Dict[str, Any]:
    import re

    m = re.search("#/definitions/(.*)$", definition_name)
    if m:
        definition_term: str = m.group(1)
        definition_dict = definitions_dict[definition_term]
        return definition_dict


def get_prefixed_name(field_prefix: Optional[str], field_name: Optional[str]) -> str:
    assert (
        field_prefix or field_name
    ), "One of field_prefix or field_name should be present"
    return (
        f"{field_prefix}.{field_name}"
        if field_prefix and field_name
        else field_name
        if not field_prefix
        else field_prefix
    )


def gen_md_table_from_struct(schema_dict: Dict[str, Any]) -> List[str]:
    table_md_str = [
        "| Field | Type | Default | Description | \n",
        "| --- | --- | --- | --- |\n",
    ]
    # table_md_str = [
    #    "<table>\n<tr>\n<td>\nField\n</td>Type<td>Default</td><td>Description</td></tr>\n"
    # ]
    gen_md_table(schema_dict, schema_dict["definitions"], md_str=table_md_str)
    # table_md_str.append("\n</table>\n")
    return table_md_str


def gen_md_table(
    field_dict: Dict[str, Any],
    definitions_dict: Dict[str, Any],
    md_str: List[str],
    field_prefix: str = None,
) -> None:
    if "enum" in field_dict:
        md_str.append(
            f"| {get_prefixed_name(field_prefix, None)} | Enum | {field_dict['type']} | one of {','.join(field_dict['enum'])} |\n"
        )

    elif "properties" in field_dict:
        for field_name, value in field_dict["properties"].items():
            if "allOf" in value:
                for sub_schema in value["allOf"]:
                    reference = sub_schema["$ref"]
                    def_dict = get_definition_dict_from_definition(
                        definitions_dict, reference
                    )
                    gen_md_table(
                        def_dict,
                        definitions_dict,
                        field_prefix=get_prefixed_name(field_prefix, field_name),
                        md_str=md_str,
                    )
            elif "type" in value and value["type"] == "enum":
                # enum
                enum_definition = value["allOf"][0]["$ref"]
                def_dict = get_definition_dict_from_definition(
                    definitions_dict, enum_definition
                )
                print(value)
                print(def_dict)
                md_str.append(
                    f"| {get_prefixed_name(field_prefix, field_name)} | Enum | {def_dict['type']} | one of {','.join(def_dict['enum'])} |\n"
                )

            elif "type" in value and value["type"] == "object":
                # struct
                if "$ref" not in value:
                    md_str.append(
                        f"| {get_prefixed_name(field_prefix, field_name)} | Dict | {value.get('default') or ''} | {value.get('description') or ''} |\n"
                    )
                else:
                    # breakpoint()
                    object_definition = value["$ref"]
                    def_dict = get_definition_dict_from_definition(
                        definitions_dict, object_definition
                    )
                    gen_md_table(
                        def_dict,
                        definitions_dict,
                        field_prefix=get_prefixed_name(field_prefix, field_name),
                        md_str=md_str,
                    )
            elif "type" in value and value["type"] == "array":
                # array
                items_type = value["items"]["type"]
                md_str.append(
                    f"| {get_prefixed_name(field_prefix, field_name)} | Array of {items_type} | {value.get('default')} | {value.get('description') or ''} | \n"
                )
                # TODO: Array of structs
            elif "type" in value:
                md_str.append(
                    f"| {get_prefixed_name(field_prefix, field_name)} | {value['type']} |  {value.get('default')} | {value.get('description') or ''} | \n"
                )
            elif "$ref" in value:
                object_definition = value["$ref"]
                def_dict = get_definition_dict_from_definition(
                    definitions_dict, object_definition
                )
                gen_md_table(
                    def_dict,
                    definitions_dict,
                    field_prefix=get_prefixed_name(field_prefix, field_name),
                    md_str=md_str,
                )
            else:
                # print(md_str, field_prefix, field_name, value)
                md_str.append(
                    f"| {get_prefixed_name(field_prefix, field_name)} | Any dict | {value.get('default')} | {value.get('description') or ''} |\n"
                )


@click.command()
@click.option("--out-dir", type=str, required=True)
def generate(out_dir: str) -> None:
    source_registry = PluginRegistry[Source]()
    source_registry.register_from_entrypoint("datahub.ingestion.source.plugins")

    source_documentation: Dict[str, Any] = {}
    # This source is always enabled
    for key in sorted(source_registry._mapping.keys()):
        # We want to attempt to load all plugins before printing a summary.
        source_type = None
        try:
            # output = subprocess.check_output(
            #    ["/bin/bash", "-c", f"pip install -e '.[{key}]'"]
            # )
            source_registry._ensure_not_lazy(key)
            print(key)
            source_type = source_registry.get(key)
            print(source_type)
        except Exception as e:
            print(f"Failed to process {key} due to {e}")
        if source_type and hasattr(source_type, "get_config_class"):
            try:
                source_config_class: ConfigModel = source_type.get_config_class()
                if hasattr(source_type, "__doc__"):
                    source_doc = textwrap.dedent(source_type.__doc__ or "")
                if hasattr(source_type, "get_platform_name"):
                    platform_name = source_type.get_platform_name()
                else:
                    platform_name = key
                config_dir = f"{out_dir}/config_schemas"
                os.makedirs(config_dir, exist_ok=True)
                # with open(f"{config_dir}/{key}_config.json", "w") as f:
                #    f.write(source_config_class.schema_json(indent=2))

                table_md = gen_md_table_from_struct(source_config_class.schema())
                source_documentation[platform_name] = (
                    source_documentation.get(platform_name) or {}
                )
                source_documentation[platform_name].update(
                    {key: {"source_doc": source_doc or "", "config": table_md}}
                )
            except Exception as e:
                raise e

    for platform, platform_docs in source_documentation.items():
        # breakpoint()
        sources_dir = f"{out_dir}/sources"
        os.makedirs(sources_dir, exist_ok=True)
        platform_doc_file = f"{sources_dir}/{platform}.md"
        with open(platform_doc_file, "w") as f:
            f.write(f"# {platform}\n")
            for plugin, plugin_docs in platform_docs.items():
                f.write(f"## Plugin {plugin}\n")
                f.write(f"{plugin_docs['source_doc'] or ''}\n")
                f.write("### Setup\n")
                f.write("```shell\n")
                f.write(f"pip install 'acryl-datahub[{plugin}]'\n")
                f.write("```\n")
                f.write("### Config Details\n")
                f.write(
                    "Note that a `.` is used to denote nested fields in the YAML recipe.\n\n"
                )
                for doc in plugin_docs["config"]:
                    f.write(doc)
            f.write(f"## Questions\n")
            f.write(
                f"If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io)\n"
            )


if __name__ == "__main__":
    logger.setLevel("INFO")
    generate()
