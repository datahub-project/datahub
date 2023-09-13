"""
Locates all the examples in the Collection and puts them in a single page.
"""

import re
from collections import defaultdict
from inspect import getmembers, isclass, isfunction
from pathlib import Path
from pkgutil import iter_modules
from textwrap import dedent
from types import ModuleType
from typing import Callable, Set, Union

import mkdocs_gen_files
from griffe.dataclasses import Docstring
from griffe.docstrings.dataclasses import DocstringSectionKind
from griffe.docstrings.parsers import Parser, parse
from prefect.logging.loggers import disable_logger
from prefect.utilities.importtools import load_module, to_qualified_name

import prefect_datahub

COLLECTION_SLUG = "prefect_datahub"


def skip_parsing(name: str, obj: Union[ModuleType, Callable], module_nesting: str):
    """
    Skips parsing the object if it's a private object or if it's not in the
    module nesting, preventing imports from other libraries from being added to the
    examples catalog.
    """
    try:
        wrong_module = not to_qualified_name(obj).startswith(module_nesting)
    except AttributeError:
        wrong_module = False
    return obj.__doc__ is None or name.startswith("_") or wrong_module


def skip_block_load_code_example(code_example: str) -> bool:
    """
    Skips the code example if it's just showing how to load a Block.
    """
    return re.search(r'\.load\("BLOCK_NAME"\)\s*$', code_example.rstrip("`"))


def get_code_examples(obj: Union[ModuleType, Callable]) -> Set[str]:
    """
    Gathers all the code examples within an object.
    """
    code_examples = set()
    with disable_logger("griffe.docstrings.google"):
        with disable_logger("griffe.agents.nodes"):
            docstring = Docstring(obj.__doc__)
            parsed_sections = parse(docstring, Parser.google)

    for section in parsed_sections:
        if section.kind == DocstringSectionKind.examples:
            code_example = "\n".join(
                (part[1] for part in section.as_dict().get("value", []))
            )
            if not skip_block_load_code_example(code_example):
                code_examples.add(code_example)
        if section.kind == DocstringSectionKind.admonition:
            value = section.as_dict().get("value", {})
            if value.get("annotation") == "example":
                code_example = value.get("description")
                if not skip_block_load_code_example(code_example):
                    code_examples.add(code_example)

    return code_examples


code_examples_grouping = defaultdict(set)
for _, module_name, ispkg in iter_modules(prefect_datahub.__path__):

    module_nesting = f"{COLLECTION_SLUG}.{module_name}"
    module_obj = load_module(module_nesting)

    # find all module examples
    if skip_parsing(module_name, module_obj, module_nesting):
        continue
    code_examples_grouping[module_name] |= get_code_examples(module_obj)

    # find all class and method examples
    for class_name, class_obj in getmembers(module_obj, isclass):
        if skip_parsing(class_name, class_obj, module_nesting):
            continue
        code_examples_grouping[module_name] |= get_code_examples(class_obj)
        for method_name, method_obj in getmembers(class_obj, isfunction):
            if skip_parsing(method_name, method_obj, module_nesting):
                continue
            code_examples_grouping[module_name] |= get_code_examples(method_obj)

    # find all function examples
    for function_name, function_obj in getmembers(module_obj, callable):
        if skip_parsing(function_name, function_obj, module_nesting):
            continue
        code_examples_grouping[module_name] |= get_code_examples(function_obj)


examples_catalog_path = Path("examples_catalog.md")
with mkdocs_gen_files.open(examples_catalog_path, "w") as generated_file:
    generated_file.write(
        dedent(
            """
            # Examples Catalog

            Below is a list of examples for `prefect-datahub`.
            """
        )
    )
    for module_name, code_examples in code_examples_grouping.items():
        if len(code_examples) == 0:
            continue
        module_title = module_name.replace("_", " ").title()
        generated_file.write(
            f"## [{module_title} Module][{COLLECTION_SLUG}.{module_name}]\n"
        )
        for code_example in code_examples:
            generated_file.write(code_example + "\n")
