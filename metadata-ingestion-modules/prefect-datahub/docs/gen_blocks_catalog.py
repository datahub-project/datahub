"""
Discovers all blocks and generates a list of them in the docs
under the Blocks Catalog heading.
"""

from pathlib import Path
from textwrap import dedent

import mkdocs_gen_files
from prefect.blocks.core import Block
from prefect.utilities.dispatch import get_registry_for_type
from prefect.utilities.importtools import from_qualified_name, to_qualified_name

COLLECTION_SLUG = "prefect_datahub"


def find_module_blocks():
    blocks = get_registry_for_type(Block)
    collection_blocks = [
        block
        for block in blocks.values()
        if to_qualified_name(block).startswith(COLLECTION_SLUG)
    ]
    module_blocks = {}
    for block in collection_blocks:
        block_name = block.__name__
        module_nesting = tuple(to_qualified_name(block).split(".")[1:-1])
        if module_nesting not in module_blocks:
            module_blocks[module_nesting] = []
        module_blocks[module_nesting].append(block_name)
    return module_blocks


def insert_blocks_catalog(generated_file):
    module_blocks = find_module_blocks()
    if len(module_blocks) == 0:
        return
    generated_file.write(
        dedent(
            f"""
            Below is a list of Blocks available for registration in
            `prefect-datahub`.

            To register blocks in this module to
            [view and edit them](https://docs.prefect.io/ui/blocks/)
            on Prefect Cloud, first [install the required packages](
            https://shubhamjagtap639.github.io/prefect-datahub/#installation),
            then
            ```bash
            prefect block register -m {COLLECTION_SLUG}
            ```
            """  # noqa
        )
    )
    generated_file.write(
        "Note, to use the `load` method on Blocks, you must already have a block document "  # noqa
        "[saved through code](https://docs.prefect.io/concepts/blocks/#saving-blocks) "  # noqa
        "or [saved through the UI](https://docs.prefect.io/ui/blocks/).\n"
    )
    for module_nesting, block_names in module_blocks.items():
        module_path = f"{COLLECTION_SLUG}." + " ".join(module_nesting)
        module_title = (
            module_path.replace(COLLECTION_SLUG, "")
            .lstrip(".")
            .replace("_", " ")
            .title()
        )
        generated_file.write(f"## [{module_title} Module][{module_path}]\n")
        for block_name in block_names:
            block_obj = from_qualified_name(f"{module_path}.{block_name}")
            block_description = block_obj.get_description()
            if not block_description.endswith("."):
                block_description += "."
            generated_file.write(
                f"[{block_name}][{module_path}.{block_name}]\n\n{block_description}\n\n"
            )
            generated_file.write(
                dedent(
                    f"""
                    To load the {block_name}:
                    ```python
                    from prefect import flow
                    from {module_path} import {block_name}

                    @flow
                    def my_flow():
                        my_block = {block_name}.load("MY_BLOCK_NAME")

                    my_flow()
                    ```
                    """
                )
            )
        generated_file.write(
            f"For additional examples, check out the [{module_title} Module]"
            f"(../examples_catalog/#{module_nesting[-1]}-module) "
            f"under Examples Catalog.\n"
        )


blocks_catalog_path = Path("blocks_catalog.md")
with mkdocs_gen_files.open(blocks_catalog_path, "w") as generated_file:
    insert_blocks_catalog(generated_file)
