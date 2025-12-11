# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import os
from typing import Dict, Type

import click
from pydantic import BaseModel

from datahub.api.entities.dataproduct.dataproduct import DataProduct
from datahub.ingestion.source.metadata.business_glossary import BusinessGlossaryConfig


@click.command()
@click.option("--out-dir", type=str, required=True)
def generate_specs(out_dir: str) -> None:
    print(out_dir)
    schemas_dir = f"{out_dir}/schemas"
    os.makedirs(schemas_dir, exist_ok=True)
    concept_class_map: Dict[str, Type[BaseModel]] = {
        "dataproduct": DataProduct,
        "businessglossary": BusinessGlossaryConfig,
    }
    for concept, concept_class in concept_class_map.items():
        with open(f"{schemas_dir}/{concept}_schema.json", "w") as f:
            f.write(concept_class.schema_json(indent=2))


if __name__ == "__main__":
    generate_specs()
