from ai.chronon.api.ttypes import EventSource, Source
from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, select
from group_bys.my_team.user_features import v1 as user_features

left_source = Source(
    events=EventSource(
        table="data.checkout_events",
        query=Query(
            selects=select(user_id="user_id"),
            time_column="ts",
        ),
    )
)

training_set = Join(
    left=left_source,
    right_parts=[JoinPart(group_by=user_features)],
    output_namespace="my_team_features",
    description="Training set joining checkout events with rolling user purchase features",
    tags={"tier": "gold"},
)
