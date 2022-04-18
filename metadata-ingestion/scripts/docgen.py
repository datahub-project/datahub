import glob
import logging
import os
import re
import textwrap
from typing import Any, Dict, List, Optional

import click
from pydantic import Field
from pydantic.dataclasses import dataclass

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.decorators import SupportStatus
from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.source import Source

logger = logging.getLogger(__name__)


@dataclass
class FieldRow:
    path: str
    type_name: str
    required: bool
    default: str
    description: str
    inner_fields: List["FieldRow"] = Field(default_factory=list)

    @staticmethod
    def get_checkbox(enabled: bool) -> str:
        return "✅" if enabled else ""

    def to_md_line(self) -> str:
        return (
            f"| {self.path} | {self.get_checkbox(self.required)} | {self.type_name} | {self.description} | {self.default} |\n"
            + "".join([inner_field.to_md_line() for inner_field in self.inner_fields])
        )


class FieldHeader(FieldRow):
    def to_md_line(self) -> str:
        return "\n".join(
            [
                "| Field | Required | Type | Description | Default |",
                "| ---   | ---      | ---  | --- | -- |",
                "",
            ]
        )

    def __init__(self):
        pass


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
        f"{field_prefix}.{field_name}"  # type: ignore
        if field_prefix and field_name
        else field_name
        if not field_prefix
        else field_prefix
    )


def gen_md_table_from_struct(schema_dict: Dict[str, Any]) -> List[str]:
    table_md_str: List[FieldRow] = []
    # table_md_str = [
    #    "<table>\n<tr>\n<td>\nField\n</td>Type<td>Default</td><td>Description</td></tr>\n"
    # ]
    gen_md_table(schema_dict, schema_dict.get("definitions", {}), md_str=table_md_str)
    # table_md_str.append("\n</table>\n")
    table_md_str.sort(key=lambda x: "z" if len(x.inner_fields) else "" + x.path)
    return (
        [FieldHeader().to_md_line()]
        + [row.to_md_line() for row in table_md_str]
        + ["\n"]
    )


def gen_md_table(
    field_dict: Dict[str, Any],
    definitions_dict: Dict[str, Any],
    md_str: List[FieldRow],
    field_prefix: str = None,
) -> None:
    if "enum" in field_dict:
        md_str.append(
            FieldRow(
                path=get_prefixed_name(field_prefix, None),
                type_name="Enum",
                required=field_dict.get("required") or False,
                description=f"one of {','.join(field_dict['enum'])}",
                default=field_dict.get("default") or "None",
            )
        )
        # md_str.append(
        #    f"| {get_prefixed_name(field_prefix, None)} | Enum | {field_dict['type']} | one of {','.join(field_dict['enum'])} |\n"
        # )

    elif "properties" in field_dict:
        for field_name, value in field_dict["properties"].items():
            # breakpoint()
            required_field: bool = field_name in field_dict.get("required", [])

            if "allOf" in value:
                for sub_schema in value["allOf"]:
                    reference = sub_schema["$ref"]
                    def_dict = get_definition_dict_from_definition(
                        definitions_dict, reference
                    )
                    row = FieldRow(
                        path=get_prefixed_name(field_prefix, field_name),
                        type_name=f"{reference.split('/')[-1]} (see below for fields)",
                        description=value.get("description") or "",
                        default=str(value.get("default")) or "",
                        required=required_field,
                    )
                    md_str.append(row)
                    # md_str.append(
                    #    f"| {get_prefixed_name(field_prefix, field_name)} | {reference.split('/')[-1]} (see below for fields) | {value.get('description') or ''} | {value.get('default') or ''} | \n"
                    # )
                    gen_md_table(
                        def_dict,
                        definitions_dict,
                        field_prefix=get_prefixed_name(field_prefix, field_name),
                        md_str=row.inner_fields,
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
                    FieldRow(
                        path=get_prefixed_name(field_prefix, field_name),
                        type_name="Enum",
                        description=f"one of {','.join(def_dict['enum'])}",
                        required=required_field,
                        default=value.get("default") or "None",
                    )
                    #                    f"| {get_prefixed_name(field_prefix, field_name)} | Enum | one of {','.join(def_dict['enum'])} | {def_dict['type']} | \n"
                )

            elif "type" in value and value["type"] == "object":
                # struct
                if "$ref" not in value:
                    md_str.append(
                        FieldRow(
                            path=get_prefixed_name(field_prefix, field_name),
                            type_name="Dict",
                            description=value.get("description") or "",
                            default=value.get("default") or "",
                            required=required_field,
                        )
                        # f"| {get_prefixed_name(field_prefix, field_name)} | Dict | {value.get('description') or ''} | {value.get('default') or ''} | \n"
                    )
                else:
                    object_definition = value["$ref"]
                    row = FieldRow(
                        path=get_prefixed_name(field_prefix, field_name),
                        type_name=f"{object_definition.split('/')[-1]} (see below for fields)",
                        description=value.get("description") or "",
                        default=value.get("default") or "",
                        required=required_field,
                    )

                    md_str.append(
                        row
                        # f"| {get_prefixed_name(field_prefix, field_name)} | {object_definition.split('/')[-1]} (see below for fields) | {value.get('description') or ''} | {value.get('default') or ''} | \n"
                    )
                    # breakpoint()
                    def_dict = get_definition_dict_from_definition(
                        definitions_dict, object_definition
                    )
                    gen_md_table(
                        def_dict,
                        definitions_dict,
                        field_prefix=get_prefixed_name(field_prefix, field_name),
                        md_str=row.inner_fields,
                    )
            elif "type" in value and value["type"] == "array":
                # array
                items_type = value["items"].get("type", "object")
                md_str.append(
                    FieldRow(
                        path=get_prefixed_name(field_prefix, field_name),
                        type_name=f"Array of {items_type}",
                        description=value.get("description") or "",
                        default=str(value.get("default")) or "None",
                        required=required_field,
                    )
                    #                    f"| {get_prefixed_name(field_prefix, field_name)} | Array of {items_type} | {value.get('description') or ''} | {value.get('default')} |  \n"
                )
                # TODO: Array of structs
            elif "type" in value:
                md_str.append(
                    FieldRow(
                        path=get_prefixed_name(field_prefix, field_name),
                        type_name=value["type"],
                        description=value.get("description") or "",
                        default=value.get("default") or "None",
                        required=required_field,
                    )
                    # f"| {get_prefixed_name(field_prefix, field_name)} | {value['type']} | {value.get('description') or ''} | {value.get('default')} | \n"
                )
            elif "$ref" in value:
                object_definition = value["$ref"]
                def_dict = get_definition_dict_from_definition(
                    definitions_dict, object_definition
                )
                row = FieldRow(
                    path=get_prefixed_name(field_prefix, field_name),
                    type_name=f"{object_definition.split('/')[-1]} (see below for fields)",
                    description=value.get("description") or "",
                    default=value.get("default") or "",
                    required=required_field,
                )

                md_str.append(
                    row
                    #    f"| {get_prefixed_name(field_prefix, field_name)} | {object_definition.split('/')[-1]} (see below for fields) | {value.get('description') or ''} | {value.get('default') or ''} | \n"
                )
                gen_md_table(
                    def_dict,
                    definitions_dict,
                    field_prefix=get_prefixed_name(field_prefix, field_name),
                    md_str=row.inner_fields,
                )
            else:
                # print(md_str, field_prefix, field_name, value)
                md_str.append(
                    FieldRow(
                        path=get_prefixed_name(field_prefix, field_name),
                        type_name="Generic dict",
                        description=value.get("description", ""),
                        default=value.get("default", "None"),
                        required=required_field,
                    )
                    # f"| {get_prefixed_name(field_prefix, field_name)} | Any dict | {value.get('description') or ''} | {value.get('default')} |\n"
                )


def get_snippet(long_string: str, max_length=100) -> str:
    snippet = ""
    if len(long_string) > max_length:
        snippet = long_string[:max_length].strip() + "... "
    else:
        snippet = long_string.strip()

    snippet = snippet.replace("\n", " ")
    snippet = snippet.strip() + " "
    return snippet


def get_support_status_badge(support_status: SupportStatus) -> str:
    if support_status == SupportStatus.CERTIFIED:
        return "![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)"
    if support_status == SupportStatus.INCUBATING:
        return "![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)"
    if support_status == SupportStatus.TESTING:
        return "![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)"

    return ""


@click.command()
@click.option("--out-dir", type=str, required=True)
@click.option("--extra-docs", type=str, required=False)
def generate(out_dir: str, extra_docs: Optional[str] = None) -> None:
    source_documentation: Dict[str, Any] = {}

    if extra_docs:
        for path in glob.glob(f"{extra_docs}/**/*.md", recursive=True):
            # breakpoint()
            m = re.search("/docs/sources/(.*)/(.*).md", path)
            if m:
                platform_name = m.group(1).lower()
                file_name = m.group(2)
                with open(path, "r") as doc_file:
                    file_contents = doc_file.read()
                    # final_markdown = preprocess_markdown(file_contents)
                    if platform_name not in source_documentation:
                        source_documentation[platform_name] = {}

                    if file_name == "README":
                        # README goes as platform level docs
                        # all other docs are assumed to be plugin level
                        source_documentation[platform_name][
                            "custom_docs"
                        ] = file_contents
                    else:
                        if "plugins" not in source_documentation[platform_name]:
                            source_documentation[platform_name]["plugins"] = {}
                        if (
                            file_name
                            not in source_documentation[platform_name]["plugins"]
                        ):
                            source_documentation[platform_name]["plugins"][
                                file_name
                            ] = {"custom_docs": file_contents}

    source_registry = PluginRegistry[Source]()
    source_registry.register_from_entrypoint("datahub.ingestion.source.plugins")

    # This source is always enabled
    for plugin_name in sorted(source_registry._mapping.keys()):
        # We want to attempt to load all plugins before printing a summary.
        source_type = None
        try:
            # output = subprocess.check_output(
            #    ["/bin/bash", "-c", f"pip install -e '.[{key}]'"]
            # )
            source_registry._ensure_not_lazy(plugin_name)
            print(plugin_name)
            source_type = source_registry.get(plugin_name)
            print(source_type)
        except Exception as e:
            print(f"Failed to process {plugin_name} due to {e}")
        if source_type and hasattr(source_type, "get_config_class"):
            try:
                source_config_class: ConfigModel = source_type.get_config_class()
                support_status = SupportStatus.UNKNOWN
                if hasattr(source_type, "__doc__"):
                    source_doc = textwrap.dedent(source_type.__doc__ or "")
                if hasattr(source_type, "get_platform_name"):
                    platform_name = source_type.get_platform_name()
                else:
                    platform_name = (
                        plugin_name.title()
                    )  # we like platform names to be human readable
                source_documentation[platform_name.lower()] = (
                    source_documentation.get(platform_name.lower()) or {}
                )

                if hasattr(source_type, "get_support_status"):
                    support_status = source_type.get_support_status()

                if "plugins" not in source_documentation[platform_name.lower()]:
                    source_documentation[platform_name.lower()]["plugins"] = {}

                if "name" not in source_documentation[platform_name.lower()]:
                    source_documentation[platform_name.lower()]["name"] = platform_name

                config_dir = f"{out_dir}/config_schemas"
                os.makedirs(config_dir, exist_ok=True)
                with open(f"{config_dir}/{plugin_name}_config.json", "w") as f:
                    f.write(source_config_class.schema_json(indent=2))

                table_md = gen_md_table_from_struct(source_config_class.schema())
                if (
                    plugin_name
                    not in source_documentation[platform_name.lower()]["plugins"]
                ):
                    source_documentation[platform_name.lower()]["plugins"][
                        plugin_name
                    ] = {}
                source_documentation[platform_name.lower()]["plugins"][
                    plugin_name
                ].update(
                    {
                        "source_doc": source_doc or "",
                        "config": table_md,
                        "support_status": support_status,
                    }
                )
            except Exception as e:
                raise e

    for platform, platform_docs in source_documentation.items():
        sources_dir = f"{out_dir}/sources"
        os.makedirs(sources_dir, exist_ok=True)
        platform_doc_file = f"{sources_dir}/{platform}.md"
        with open(platform_doc_file, "w") as f:
            f.write(f"# {platform_docs['name']}\n")
            if len(platform_docs["plugins"].keys()) > 1:
                # More than one plugin used to provide integration with this platform
                f.write(
                    f"There are {len(platform_docs['plugins'].keys())} sources that provide integration with {platform_docs['name']}\n"
                )
                f.write("\n")
                f.write(f"| Source Module | Documentation |\n")
                f.write(f"| ------ | ---- |\n")
                for plugin in platform_docs["plugins"]:
                    f.write(
                        f"| `{plugin}` | {get_snippet(platform_docs['plugins'][plugin]['source_doc'])}[Read more...](#module-{plugin}) |\n"
                    )
                f.write("\n")
            # insert platform level custom docs before plugin section
            f.write(platform_docs.get("custom_docs") or "")
            for plugin, plugin_docs in platform_docs["plugins"].items():
                f.write(f"## Module `{plugin}`\n")
                if "support_status" in plugin_docs:
                    f.write(
                        get_support_status_badge(plugin_docs["support_status"]) + "\n"
                    )
                f.write(f"{plugin_docs['source_doc'] or ''}\n")
                f.write("### Setup\n")
                f.write("```shell\n")
                f.write(f"pip install 'acryl-datahub[{plugin}]'\n")
                f.write("```\n")
                # insert custom plugin docs before config details
                f.write(plugin_docs.get("custom_docs", ""))
                f.write("\n### Config Details\n")
                f.write(
                    "Note that a `.` is used to denote nested fields in the YAML recipe.\n\n"
                )
                for doc in plugin_docs["config"]:
                    f.write(doc)

            f.write("## Questions\n")
            f.write(
                "If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io)\n"
            )


if __name__ == "__main__":
    logger.setLevel("INFO")
    generate()
