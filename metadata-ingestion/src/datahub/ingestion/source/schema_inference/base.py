# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import IO, List

from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField


class SchemaInferenceBase:
    """
    Base class for file schema inference.
    """

    def infer_schema(self, file: IO[bytes]) -> List[SchemaField]:
        """
        Infer schema from file.
        """
        raise NotImplementedError("infer_schema not implemented")
