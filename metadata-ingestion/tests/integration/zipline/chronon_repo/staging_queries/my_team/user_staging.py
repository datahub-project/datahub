from ai.chronon.api.ttypes import MetaData, StagingQuery

query = """
SELECT
    user_id,
    purchase_amount,
    item_id,
    ts,
    ds
FROM data.raw_purchases
WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
"""

v1 = StagingQuery(
    metaData=MetaData(
        outputNamespace="my_team_features",
        description="Cleans raw purchase events into a curated table for downstream GroupBys",
        tableProperties={"tier": "gold"},
    ),
    query=query,
    startPartition="2023-01-01",
)
