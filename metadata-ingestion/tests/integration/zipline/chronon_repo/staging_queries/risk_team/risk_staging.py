from ai.chronon.api.ttypes import MetaData, StagingQuery

query = """
SELECT
    a.account_id,
    a.balance,
    p.purchase_amount
FROM warehouse.accounts a
JOIN data.raw_purchases p ON a.account_id = p.user_id
WHERE a.ds = '{{ latest_date }}'
"""

v1 = StagingQuery(
    metaData=MetaData(
        outputNamespace="risk_features",
        description="Joins warehouse accounts with raw purchases to build a risk staging table",
        tableProperties={"tier": "silver"},
    ),
    query=query,
    startPartition="2023-01-01",
)
