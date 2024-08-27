import contextlib
import pathlib
from typing import Any, Iterator

import ruamel.yaml.util
from ruamel.yaml import YAML


@contextlib.contextmanager
def YamlFileUpdater(file: pathlib.Path) -> Iterator[Any]:
    yaml = YAML()
    yaml.preserve_quotes = True  # type: ignore[assignment]

    doc = yaml.load(file)

    # All the user to make changes to the doc.
    # TODO: Enable replacing the doc entirely.
    yield doc

    # Guess existing indentation in the file so that we can preserve it.
    _, ind, bsi = ruamel.yaml.util.load_yaml_guess_indent(file.read_text())
    yaml.width = 2**20  # type: ignore[assignment]

    yaml.sequence_indent = ind
    yaml.block_seq_indent = bsi

    if (ind, bsi) == (4, 2):
        # (2, 4, 2) is much more common than (4, 4, 2).
        yaml.map_indent = 2  # type: ignore[assignment]
    else:
        # TODO: Some folks use a different mapping indent than sequence indent.
        # We should support that, but for now, we just use the sequence indent.
        yaml.map_indent = ind

    yaml.dump(doc, file)
