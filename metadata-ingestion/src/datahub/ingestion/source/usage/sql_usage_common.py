def sql_compatibility_change(row):
    # Make some minor type conversions.
    if hasattr(row, "_asdict"):
        # Compat with SQLAlchemy 1.3 and 1.4
        # See https://docs.sqlalchemy.org/en/14/changelog/migration_14.html#rowproxy-is-no-longer-a-proxy-is-now-called-row-and-behaves-like-an-enhanced-named-tuple.
        event_dict = row._asdict()
    else:
        event_dict = dict(row)

    return event_dict
