import re
from typing import Type

import pytest
from prefect.blocks.core import Block

from prefect_datahub.datahub_emitter import DatahubEmitter


@pytest.mark.parametrize("block", [DatahubEmitter])
class TestAllBlocksAdhereToStandards:
    @pytest.fixture
    def block(self, block):
        return block

    def test_has_a_description(self, block: Type[Block]) -> None:
        assert block.get_description()

    def test_has_a_valid_code_example(self, block: Type[Block]) -> None:
        code_example = block.get_code_example()
        assert code_example is not None, f"{block.__name__} is missing a code example"
        import_pattern = rf"from .* import {block.__name__}"
        assert re.search(import_pattern, code_example) is not None, (
            f"The code example for {block.__name__} is missing an import statement"
            f" matching the pattern {import_pattern}"
        )
        block_load_pattern = rf'.* = {block.__name__}\.load\("BLOCK_NAME"\)'
        assert re.search(block_load_pattern, code_example), (
            f"The code example for {block.__name__} is missing a .load statement"
            f" matching the pattern {block_load_pattern}"
        )
