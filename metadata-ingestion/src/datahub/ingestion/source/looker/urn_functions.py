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
