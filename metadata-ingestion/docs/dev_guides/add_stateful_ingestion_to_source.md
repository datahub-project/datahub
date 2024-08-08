# Adding Stateful Ingestion to a Source

Currently, datahub supports the [Stale Metadata Removal](./stateful.md#stale-entity-removal) and
the [Redunant Run Elimination](./stateful.md#redundant-run-elimination) use-cases on top of the more generic stateful ingestion
capability available for the sources. This document describes how to add support for these two use-cases to new sources.

## Adding Stale Metadata Removal to a Source

Adding the stale metadata removal use-case to a new source involves modifying the source config, source report, and the source itself.

For a full example of all changes required: [Adding stale metadata removal to the MongoDB source](https://github.com/datahub-project/datahub/pull/9118).

The [datahub.ingestion.source.state.stale_entity_removal_handler](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/state/stale_entity_removal_handler.py) module provides the supporting infrastructure for all the steps described
above and substantially simplifies the implementation on the source side. Below is a detailed explanation of each of these
steps along with examples.

### 1. Modify the source config

The source's config must inherit from `StatefulIngestionConfigBase`, and should declare a field named `stateful_ingestion` of type `Optional[StatefulStaleMetadataRemovalConfig]`.

Example:

```python
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
    StatefulIngestionConfigBase,
)

class MySourceConfig(StatefulIngestionConfigBase):
    # ...<other config params>...

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None
```

### 2. Modify the source report

The report class of the source should inherit from `StaleEntityRemovalSourceReport` instead of `SourceReport`.

```python
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)

@dataclass
class MySourceReport(StatefulIngestionReport):
    # <other fields here>
    pass
```

### 3. Modify the source

1. The source must inherit from `StatefulIngestionSourceBase` instead of `Source`.
2. The source should contain a custom `get_workunit_processors` method.

```python
from datahub.ingestion.source.state.stateful_ingestion_base import StatefulIngestionSourceBase
from datahub.ingestion.source.state.stale_entity_removal_handler import StaleEntityRemovalHandler

class MySource(StatefulIngestionSourceBase):
    def __init__(self, config: MySourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)

        self.config = config
        self.report = MySourceReport()

        # other initialization code here

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    # other methods here
```

## Adding Redundant Run Elimination to a Source

This use-case applies to the sources that drive ingestion by querying logs over a specified duration via the config(such
as snowflake usage, bigquery usage etc.). It typically involves expensive and long-running queries. To add redundant
run elimination to a new source to prevent the expensive reruns for the same time range(potentially due to a user
error or a scheduler malfunction), the following steps
are required.

1. Update the `SourceConfig`
2. Update the `SourceReport`
3. Modify the `Source` to
   1. Instantiate the RedundantRunSkipHandler object.
   2. Check if the current run should be skipped.
   3. Update the state for the current run(start & end times).

The [datahub.ingestion.source.state.redundant_run_skip_handler](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/state/redundant_run_skip_handler.py)
modules provides the supporting infrastructure required for all the steps described above.

NOTE: The handler currently uses a simple state,
the [BaseUsageCheckpointState](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/state/usage_common_state.py),
across all sources it supports (unlike the StaleEntityRemovalHandler).

### 1. Modifying the SourceConfig

The `SourceConfig` must inherit from the [StatefulRedundantRunSkipConfig](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/state/redundant_run_skip_handler.py#L23) class.

Examples:

1. Snowflake Usage

```python
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    StatefulRedundantRunSkipConfig,
)
class SnowflakeStatefulIngestionConfig(StatefulRedundantRunSkipConfig):
    pass
```

### 2. Modifying the SourceReport

The `SourceReport` must inherit from the [StatefulIngestionReport](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/state/stateful_ingestion_base.py#L102) class.
Examples:

1. Snowflake Usage

```python
@dataclass
class SnowflakeUsageReport(BaseSnowflakeReport, StatefulIngestionReport):
    # <members specific to snowflake usage report>
```

### 3. Modifying the Source

The source must inherit from `StatefulIngestionSourceBase`.

#### 3.1 Instantiate RedundantRunSkipHandler in the `__init__` method of the source.

The source should instantiate an instance of the `RedundantRunSkipHandler` in its `__init__` method.
Examples:
Snowflake Usage

```python
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantRunSkipHandler,
)
class SnowflakeUsageSource(StatefulIngestionSourceBase):

    def __init__(self, config: SnowflakeUsageConfig, ctx: PipelineContext):
        super(SnowflakeUsageSource, self).__init__(config, ctx)
        self.config: SnowflakeUsageConfig = config
        self.report: SnowflakeUsageReport = SnowflakeUsageReport()
        # Create and register the stateful ingestion use-case handlers.
        self.redundant_run_skip_handler = RedundantRunSkipHandler(
            source=self,
            config=self.config,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )
```

#### 3.2 Checking if the current run should be skipped.

The sources can query if the current run should be skipped using `should_skip_this_run` method of `RedundantRunSkipHandler`. This should done from the `get_workunits` method, before doing any other work.

Example code:

```python
def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # Skip a redundant run
        if self.redundant_run_skip_handler.should_skip_this_run(
            cur_start_time_millis=datetime_to_ts_millis(self.config.start_time)
        ):
            return
        # Generate the workunits.
```

#### 3.3 Updating the state for the current run.

The source should use the `update_state` method of `RedundantRunSkipHandler` to update the current run's state if the run has not been skipped. This step can be performed in the `get_workunits` if the run has not been skipped.

Example code:

```python
    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # Skip a redundant run
        if self.redundant_run_skip_handler.should_skip_this_run(
            cur_start_time_millis=self.config.start_time
        ):
            return

        # Generate the workunits.
        # <code for generating the workunits>
        # Update checkpoint state for this run.
        self.redundant_run_skip_handler.update_state(
            start_time_millis=self.config.start_time,
            end_time_millis=self.config.end_time,
        )
```
