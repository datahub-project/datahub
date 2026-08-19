from ai.chronon.api.ttypes import EventSource, Source
from ai.chronon.group_by import Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query, select

source = Source(
    events=EventSource(
        table="data.user_purchases",
        topic="events.user_purchases",
        query=Query(
            selects=select(
                user_id="user_id",
                purchase_amount="purchase_amount",
                item_id="item_id",
            ),
            time_column="ts",
        ),
    )
)

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    aggregations=[
        Aggregation(
            input_column="purchase_amount",
            operation=Operation.SUM,
            windows=[
                Window(length=3, timeUnit=TimeUnit.DAYS),
                Window(length=7, timeUnit=TimeUnit.DAYS),
            ],
        ),
        Aggregation(
            input_column="purchase_amount",
            operation=Operation.AVERAGE,
            windows=[Window(length=7, timeUnit=TimeUnit.DAYS)],
        ),
        Aggregation(
            input_column="item_id",
            operation=Operation.APPROX_UNIQUE_COUNT,
            windows=[Window(length=30, timeUnit=TimeUnit.DAYS)],
        ),
    ],
    online=False,
    description="Rolling purchase behaviour features keyed on user_id",
    tags={"tier": "gold", "pii": "false"},
)
