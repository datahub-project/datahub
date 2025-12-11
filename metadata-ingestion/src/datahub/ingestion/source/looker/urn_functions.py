# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

def get_qualified_table_name(urn: str) -> str:
    part: str = urn.split(",")[-2]

    if len(part.split(".")) >= 4:
        return ".".join(
            part.split(".")[-3:]
        )  # return only db.schema.table skip platform instance as higher code is
        # failing if encounter platform-instance in qualified table name
    else:
        return part


def get_table_name(urn: str) -> str:
    qualified_table_name: str = get_qualified_table_name(
        urn=urn,
    )

    return qualified_table_name.split(".")[-1]
