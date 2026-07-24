from ai.chronon.api.ttypes import EntitySource, Source
from ai.chronon.group_by import Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query, select

source = Source(
    entities=EntitySource(
        snapshotTable="warehouse.accounts",
        mutationTopic="events.account_updates",
        query=Query(
            selects=select(
                account_id="account_id",
                risk_score="risk_score",
                status="status",
                balance="balance",
                country="country",
            ),
            time_column="ts",
        ),
    )
)

v1 = GroupBy(
    sources=[source],
    keys=["account_id"],
    aggregations=[
        Aggregation(
            input_column="risk_score",
            operation=Operation.TOP_K(5),
            windows=[Window(length=7, timeUnit=TimeUnit.DAYS)],
            tags={"sensitivity": "high"},
        ),
        Aggregation(
            input_column="status",
            operation=Operation.LAST,
        ),
        Aggregation(
            input_column="balance",
            operation=Operation.SUM,
            windows=[Window(length=30, timeUnit=TimeUnit.DAYS)],
            buckets=["country"],
        ),
    ],
    online=True,
    description="Account risk features keyed on account_id, backed by the warehouse snapshot",
    tags={"tier": "silver"},
)
