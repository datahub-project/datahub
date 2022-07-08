import glob
import json
import logging
import os
import re
import sys
import textwrap
from importlib.metadata import metadata, requires
from typing import Any, Dict, List, Optional

import click
from pydantic import Field
from pydantic.dataclasses import dataclass

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.decorators import (
    CapabilitySetting,
    SourceCapability,
    SupportStatus,
)
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

    raise Exception("Failed to find a definition for " + definition_name)


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

    table_md_str = [field for field in table_md_str if len(field.inner_fields) == 0] + [
        field for field in table_md_str if len(field.inner_fields) > 0
    ]

    # table_md_str.sort(key=lambda x: "z" if len(x.inner_fields) else "" + x.path)
    return (
        [FieldHeader().to_md_line()]
        + [row.to_md_line() for row in table_md_str]
        + ["\n"]
    )


def get_enum_description(
    authored_description: Optional[str], enum_symbols: List[str]
) -> str:
    description = authored_description or ""
    missed_symbols = [symbol for symbol in enum_symbols if symbol not in description]
    if missed_symbols:
        description = (
            description + "."
            if description
            else "" + " Allowed symbols are " + ",".join(enum_symbols)
        )

    return description


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
                default=str(field_dict.get("default", "None")),
            )
        )
        # md_str.append(
        #    f"| {get_prefixed_name(field_prefix, None)} | Enum | {field_dict['type']} | one of {','.join(field_dict['enum'])} |\n"
        # )

    elif "properties" in field_dict:
        for field_name, value in field_dict["properties"].items():
            required_field: bool = field_name in field_dict.get("required", [])

            if "allOf" in value:
                for sub_schema in value["allOf"]:
                    reference = sub_schema["$ref"]
                    def_dict = get_definition_dict_from_definition(
                        definitions_dict, reference
                    )
                    # special case for enum reference, we don't split up the rows
                    if "enum" in def_dict:
                        row = FieldRow(
                            path=get_prefixed_name(field_prefix, field_name),
                            type_name=f"enum({reference.split('/')[-1]})",
                            description=get_enum_description(
                                value.get("description"), def_dict["enum"]
                            ),
                            default=str(value.get("default", "")),
                            required=required_field,
                        )
                        md_str.append(row)
                    else:
                        # object reference
                        row = FieldRow(
                            path=get_prefixed_name(field_prefix, field_name),
                            type_name=f"{reference.split('/')[-1]} (see below for fields)",
                            description=value.get("description") or "",
                            default=str(value.get("default", "")),
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
                        default=str(value.get("default", "None")),
                    )
                    #                    f"| {get_prefixed_name(field_prefix, field_name)} | Enum | one of {','.join(def_dict['enum'])} | {def_dict['type']} | \n"
                )

            elif "type" in value and value["type"] == "object":
                # struct
                if "$ref" not in value:
                    if (
                        "additionalProperties" in value
                        and "$ref" in value["additionalProperties"]
                    ):
                        # breakpoint()
                        value_ref = value["additionalProperties"]["$ref"]
                        def_dict = get_definition_dict_from_definition(
                            definitions_dict, value_ref
                        )

                        row = FieldRow(
                            path=get_prefixed_name(field_prefix, field_name),
                            type_name=f"Dict[str, {value_ref.split('/')[-1]}]",
                            description=value.get("description") or "",
                            default=str(value.get("default", "")),
                            required=required_field,
                        )
                        md_str.append(row)
                        gen_md_table(
                            def_dict,
                            definitions_dict,
                            field_prefix=get_prefixed_name(
                                field_prefix, f"{field_name}.`key`"
                            ),
                            md_str=row.inner_fields,
                        )
                    else:
                        value_type = value.get("additionalProperties", {}).get("type")
                        md_str.append(
                            FieldRow(
                                path=get_prefixed_name(field_prefix, field_name),
                                type_name=f"Dict[str,{value_type}]"
                                if value_type
                                else "Dict",
                                description=value.get("description") or "",
                                default=str(value.get("default", "")),
                                required=required_field,
                            )
                        )
                else:
                    object_definition = value["$ref"]
                    row = FieldRow(
                        path=get_prefixed_name(field_prefix, field_name),
                        type_name=f"{object_definition.split('/')[-1]} (see below for fields)",
                        description=value.get("description") or "",
                        default=str(value.get("default", "")),
                        required=required_field,
                    )

                    md_str.append(
                        row
                        # f"| {get_prefixed_name(field_prefix, field_name)} | {object_definition.split('/')[-1]} (see below for fields) | {value.get('description') or ''} | {value.get('default') or ''} | \n"
                    )
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
                        default=str(value.get("default", "None")),
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
                        default=str(value.get("default", "None")),
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
                    default=str(value.get("default", "")),
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
                        default=str(value.get("default", "None")),
                        required=required_field,
                    )
                    # f"| {get_prefixed_name(field_prefix, field_name)} | Any dict | {value.get('description') or ''} | {value.get('default')} |\n"
                )


def get_snippet(long_string: str, max_length: int = 100) -> str:
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


def get_capability_supported_badge(supported: bool) -> str:
    return "✅" if supported else "❌"


def get_capability_text(src_capability: SourceCapability) -> str:
    """
    Returns markdown format cell text for a capability, hyperlinked to capability feature page if known
    """
    capability_docs_mapping: Dict[SourceCapability, str] = {
        SourceCapability.DELETION_DETECTION: "../../../../metadata-ingestion/docs/dev_guides/stateful.md#removal-of-stale-tables-and-views",
        SourceCapability.DOMAINS: "../../../domains.md",
        SourceCapability.PLATFORM_INSTANCE: "../../../platform-instances.md",
        SourceCapability.DATA_PROFILING: "../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md",
    }

    capability_doc = capability_docs_mapping.get(src_capability)
    return (
        src_capability.value
        if not capability_doc
        else f"[{src_capability.value}]({capability_doc})"
    )


def create_or_update(
    something: Dict[Any, Any], path: List[str], value: Any
) -> Dict[Any, Any]:
    dict_under_operation = something
    for p in path[:-1]:
        if p not in dict_under_operation:
            dict_under_operation[p] = {}
        dict_under_operation = dict_under_operation[p]

    dict_under_operation[path[-1]] = value
    return something


def does_extra_exist(extra_name: str) -> bool:
    for key, value in metadata("acryl-datahub").items():
        if key == "Provides-Extra" and value == extra_name:
            return True
    return False


def get_additional_deps_for_extra(extra_name: str) -> List[str]:
    all_requirements = requires("acryl-datahub") or []
    # filter for base dependencies
    base_deps = set([x.split(";")[0] for x in all_requirements if "extra ==" not in x])
    # filter for dependencies for this extra
    extra_deps = set(
        [x.split(";")[0] for x in all_requirements if f'extra == "{extra_name}"' in x]
    )
    # calculate additional deps that this extra adds
    delta_deps = extra_deps - base_deps
    return list(delta_deps)


def relocate_path(orig_path: str, relative_path: str, relocated_path: str) -> str:

    newPath = os.path.join(os.path.dirname(orig_path), relative_path)
    assert os.path.exists(newPath)

    newRelativePath = os.path.relpath(newPath, os.path.dirname(relocated_path))
    return newRelativePath


def rewrite_markdown(file_contents: str, path: str, relocated_path: str) -> str:
    def new_url(original_url: str, file_path: str) -> str:
        if original_url.startswith(("http://", "https://", "#")):
            return original_url
        import pathlib

        file_ext = pathlib.Path(original_url).suffix
        if file_ext.startswith(".md"):
            return original_url
        elif file_ext in [".png", ".svg", ".gif", ".pdf"]:
            new_url = relocate_path(path, original_url, relocated_path)
            return new_url
        return original_url

    # Look for the [text](url) syntax. Note that this will also capture images.
    #
    # We do a little bit of parenthesis matching here to account for parens in URLs.
    # See https://stackoverflow.com/a/17759264 for explanation of the second capture group.
    new_content = re.sub(
        r"\[(.*?)\]\(((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*)\)",
        lambda x: f"[{x.group(1)}]({new_url(x.group(2).strip(),path)})",  # type: ignore
        file_contents,
    )

    new_content = re.sub(
        # Also look for the [text]: url syntax.
        r"^\[(.+?)\]\s*:\s*(.+?)\s*$",
        lambda x: f"[{x.group(1)}]: {new_url(x.group(2), path)}",
        new_content,
    )
    return new_content


@click.command()
@click.option("--out-dir", type=str, required=True)
@click.option("--extra-docs", type=str, required=False)
@click.option("--source", type=str, required=False)
def generate(
    out_dir: str, extra_docs: Optional[str] = None, source: Optional[str] = None
) -> None:  # noqa: C901
    source_documentation: Dict[str, Any] = {}
    metrics = {}
    metrics["source_platforms"] = {"discovered": 0, "generated": 0, "warnings": []}
    metrics["plugins"] = {"discovered": 0, "generated": 0, "failed": 0}

    if extra_docs:
        for path in glob.glob(f"{extra_docs}/**/*[.md|.yaml|.yml]", recursive=True):
            # breakpoint()

            m = re.search("/docs/sources/(.*)/(.*).md", path)
            if m:
                platform_name = m.group(1).lower()
                file_name = m.group(2)
                destination_md: str = (
                    f"../docs/generated/ingestion/sources/{platform_name}.md"
                )

                with open(path, "r") as doc_file:
                    file_contents = doc_file.read()
                    final_markdown = rewrite_markdown(
                        file_contents, path, destination_md
                    )

                    if file_name == "README":
                        # README goes as platform level docs
                        # all other docs are assumed to be plugin level
                        create_or_update(
                            source_documentation,
                            [platform_name, "custom_docs"],
                            final_markdown,
                        )
                    else:
                        create_or_update(
                            source_documentation,
                            [platform_name, "plugins", file_name, "custom_docs"],
                            final_markdown,
                        )
            else:
                yml_match = re.search("/docs/sources/(.*)/(.*)_recipe.yml", path)
                if yml_match:
                    platform_name = yml_match.group(1).lower()
                    plugin_name = yml_match.group(2)
                    with open(path, "r") as doc_file:
                        file_contents = doc_file.read()
                        create_or_update(
                            source_documentation,
                            [platform_name, "plugins", plugin_name, "recipe"],
                            file_contents,
                        )

    source_registry = PluginRegistry[Source]()
    source_registry.register_from_entrypoint("datahub.ingestion.source.plugins")

    # This source is always enabled
    for plugin_name in sorted(source_registry._mapping.keys()):
        if source and source != plugin_name:
            continue

        metrics["plugins"]["discovered"] = metrics["plugins"]["discovered"] + 1
        # We want to attempt to load all plugins before printing a summary.
        source_type = None
        try:
            # output = subprocess.check_output(
            #    ["/bin/bash", "-c", f"pip install -e '.[{key}]'"]
            # )
            class_or_exception = source_registry._ensure_not_lazy(plugin_name)
            if isinstance(class_or_exception, Exception):
                raise class_or_exception
            logger.debug(f"Processing {plugin_name}")
            source_type = source_registry.get(plugin_name)
            logger.debug(f"Source class is {source_type}")
            extra_plugin = plugin_name if does_extra_exist(plugin_name) else None
            extra_deps = (
                get_additional_deps_for_extra(extra_plugin) if extra_plugin else []
            )
        except Exception as e:
            print(f"Failed to process {plugin_name} due to exception")
            print(repr(e))
            metrics["plugins"]["failed"] = metrics["plugins"].get("failed", 0) + 1

        if source_type and hasattr(source_type, "get_config_class"):
            try:
                source_config_class: ConfigModel = source_type.get_config_class()
                support_status = SupportStatus.UNKNOWN
                capabilities = []
                if hasattr(source_type, "__doc__"):
                    source_doc = textwrap.dedent(source_type.__doc__ or "")
                if hasattr(source_type, "get_platform_name"):
                    platform_name = source_type.get_platform_name()
                else:
                    platform_name = (
                        plugin_name.title()
                    )  # we like platform names to be human readable

                if hasattr(source_type, "get_platform_id"):
                    platform_id = source_type.get_platform_id()

                source_documentation[platform_id] = (
                    source_documentation.get(platform_id) or {}
                )
                # breakpoint()

                create_or_update(
                    source_documentation,
                    [platform_id, "plugins", plugin_name, "classname"],
                    ".".join([source_type.__module__, source_type.__name__]),
                )
                plugin_file_name = "src/" + "/".join(source_type.__module__.split("."))
                if os.path.exists(plugin_file_name) and os.path.isdir(plugin_file_name):
                    plugin_file_name = plugin_file_name + "/__init__.py"
                else:
                    plugin_file_name = plugin_file_name + ".py"
                if os.path.exists(plugin_file_name):
                    create_or_update(
                        source_documentation,
                        [platform_id, "plugins", plugin_name, "filename"],
                        plugin_file_name,
                    )
                else:
                    logger.info(
                        f"Failed to locate filename for {plugin_name}. Guessed {plugin_file_name}"
                    )

                if hasattr(source_type, "get_support_status"):
                    support_status = source_type.get_support_status()

                if hasattr(source_type, "get_capabilities"):
                    capabilities = list(source_type.get_capabilities())
                    capabilities.sort(key=lambda x: x.capability.value)

                create_or_update(
                    source_documentation,
                    [platform_id, "plugins", plugin_name, "capabilities"],
                    capabilities,
                )

                create_or_update(
                    source_documentation, [platform_id, "name"], platform_name
                )

                create_or_update(
                    source_documentation,
                    [platform_id, "plugins", plugin_name, "extra_deps"],
                    extra_deps,
                )

                config_dir = f"{out_dir}/config_schemas"
                os.makedirs(config_dir, exist_ok=True)
                with open(f"{config_dir}/{plugin_name}_config.json", "w") as f:
                    f.write(source_config_class.schema_json(indent=2))

                create_or_update(
                    source_documentation,
                    [platform_id, "plugins", plugin_name, "config_schema"],
                    source_config_class.schema_json(indent=2) or "",
                )

                table_md = gen_md_table_from_struct(source_config_class.schema())
                create_or_update(
                    source_documentation,
                    [platform_id, "plugins", plugin_name, "source_doc"],
                    source_doc or "",
                )
                create_or_update(
                    source_documentation,
                    [platform_id, "plugins", plugin_name, "config"],
                    table_md,
                )
                create_or_update(
                    source_documentation,
                    [platform_id, "plugins", plugin_name, "support_status"],
                    support_status,
                )

            except Exception as e:
                raise e

    sources_dir = f"{out_dir}/sources"
    os.makedirs(sources_dir, exist_ok=True)

    for platform_id, platform_docs in source_documentation.items():
        if source and platform_id != source:
            continue
        metrics["source_platforms"]["discovered"] = (
            metrics["source_platforms"]["discovered"] + 1
        )
        platform_doc_file = f"{sources_dir}/{platform_id}.md"
        if "name" not in platform_docs:
            # We seem to have discovered written docs that corresponds to a platform, but haven't found linkage to it from the source classes
            warning_msg = f"Failed to find source classes for platform {platform_id}. Did you remember to annotate your source class with @platform_name({platform_id})?"
            logger.error(warning_msg)
            metrics["source_platforms"]["warnings"].append(warning_msg)

        with open(platform_doc_file, "w") as f:
            if "name" in platform_docs:
                f.write(
                    f"import Tabs from '@theme/Tabs';\nimport TabItem from '@theme/TabItem';\n\n"
                )
                f.write(f"# {platform_docs['name']}\n")
            if len(platform_docs["plugins"].keys()) > 1:
                # More than one plugin used to provide integration with this platform
                f.write(
                    f"There are {len(platform_docs['plugins'].keys())} sources that provide integration with {platform_docs['name']}\n"
                )
                f.write("\n")
                f.write("<table>\n")
                f.write("<tr>")
                for col_header in ["Source Module", "Documentation"]:
                    f.write(f"<td>{col_header}</td>")
                f.write("</tr>")

                #                f.write("| Source Module | Documentation |\n")
                #                f.write("| ------ | ---- |\n")
                for plugin in sorted(platform_docs["plugins"]):
                    f.write("<tr>\n")
                    f.write(f"<td>\n\n`{plugin}`\n\n</td>\n")
                    f.write(
                        f"<td>\n\n\n{platform_docs['plugins'][plugin].get('source_doc') or ''} [Read more...](#module-{plugin})\n\n\n</td>\n"
                    )
                    f.write("</tr>\n")
                #                    f.write(
                #                        f"| `{plugin}` | {get_snippet(platform_docs['plugins'][plugin]['source_doc'])}[Read more...](#module-{plugin}) |\n"
                #                    )
                f.write("</table>\n\n")
            # insert platform level custom docs before plugin section
            f.write(platform_docs.get("custom_docs") or "")
            for plugin in sorted(platform_docs["plugins"]):
                plugin_docs = platform_docs["plugins"][plugin]
                f.write(f"\n\n## Module `{plugin}`\n")
                if "support_status" in plugin_docs:
                    f.write(
                        get_support_status_badge(plugin_docs["support_status"]) + "\n\n"
                    )
                if "capabilities" in plugin_docs and len(plugin_docs["capabilities"]):
                    f.write("\n### Important Capabilities\n")
                    f.write("| Capability | Status | Notes |\n")
                    f.write("| ---------- | ------ | ----- |\n")
                    plugin_capabilities: List[CapabilitySetting] = plugin_docs[
                        "capabilities"
                    ]
                    for cap_setting in plugin_capabilities:
                        f.write(
                            f"| {get_capability_text(cap_setting.capability)} | {get_capability_supported_badge(cap_setting.supported)} | {cap_setting.description} |\n"
                        )
                    f.write("\n")

                f.write(f"{plugin_docs.get('source_doc') or ''}\n")
                if "extra_deps" in plugin_docs:
                    f.write("### Install the Plugin\n")
                    if plugin_docs["extra_deps"] != []:
                        f.write("```shell\n")
                        f.write(f"pip install 'acryl-datahub[{plugin}]'\n")
                        f.write("```\n")
                    else:
                        f.write(
                            f"The `{plugin}` source works out of the box with `acryl-datahub`.\n"
                        )
                if "recipe" in plugin_docs:
                    f.write("\n### Quickstart Recipe\n")
                    f.write(
                        "Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.\n\n\n"
                    )
                    f.write(
                        "For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes)\n"
                    )
                    f.write("```yaml\n")
                    f.write(plugin_docs["recipe"])
                    f.write("\n```\n")
                if "config" in plugin_docs:
                    f.write("\n### Config Details\n")
                    f.write(
                        """<Tabs>
                <TabItem value="options" label="Options" default>\n\n"""
                    )
                    f.write(
                        "Note that a `.` is used to denote nested fields in the YAML recipe.\n\n"
                    )
                    f.write(
                        "\n<details open>\n<summary>View All Configuration Options</summary>\n\n"
                    )
                    for doc in plugin_docs["config"]:
                        f.write(doc)
                    f.write("\n</details>\n\n")
                    f.write(
                        f"""</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.\n\n
```javascript
{plugin_docs['config_schema']}
```\n\n
</TabItem>
</Tabs>\n\n"""
                    )
                # insert custom plugin docs after config details
                f.write(plugin_docs.get("custom_docs", ""))
                if "classname" in plugin_docs:
                    f.write("\n### Code Coordinates\n")
                    f.write(f"- Class Name: `{plugin_docs['classname']}`\n")
                    if "filename" in plugin_docs:
                        f.write(
                            f"- Browse on [GitHub](../../../../metadata-ingestion/{plugin_docs['filename']})\n\n"
                        )
                metrics["plugins"]["generated"] = metrics["plugins"]["generated"] + 1

            f.write("\n## Questions\n")
            f.write(
                f"If you've got any questions on configuring ingestion for {platform_docs.get('name',platform_id)}, feel free to ping us on [our Slack](https://slack.datahubproject.io)\n"
            )
            metrics["source_platforms"]["generated"] = (
                metrics["source_platforms"]["generated"] + 1
            )
    print("Ingestion Documentation Generation Complete")
    print("############################################")
    print(json.dumps(metrics, indent=2))
    print("############################################")
    if metrics["plugins"].get("failed", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    logger.setLevel("INFO")
    generate()
