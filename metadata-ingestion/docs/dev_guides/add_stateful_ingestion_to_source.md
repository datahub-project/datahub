# Adding Stateful Ingestion to a Source

Currently, datahub supports the [Stale Metadata Removal](./stateful.md#stale-entity-removal) and
the [Redunant Run Elimination](./stateful.md#redundant-run-elimination) use-cases on top of the more generic stateful ingestion
capability available for the sources. This document describes how to add support for these two use-cases to new sources.

## Adding Stale Metadata Removal to a Source
Adding the stale metadata removal use-case to a new source involves
1. Defining the new checkpoint state that stores the list of entities emitted from a specific ingestion run.
2. Modifying the `SourceConfig` associated with the source to use a custom `stateful_ingestion` config param.
3. Modifying the `SourceReport` associated with the source to include soft-deleted entities in the report.
4. Modifying the `Source` to
   1. Instantiate the StaleEntityRemovalHandler object
   2. Add entities from the current run to the state object
   3. Emit stale metadata removal workunits

The [datahub.ingestion.source.state.stale_entity_removal_handler](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/state/stale_entity_removal_handler.py) module provides the supporting infrastructure for all the steps described
above and substantially simplifies the implementation on the source side. Below is a detailed explanation of each of these
steps along with examples.

### 1. Defining the checkpoint state for the source.
The checkpoint state class is responsible for tracking the entities emitted from each ingestion run. If none of the existing states do not meet the needs of the new source, a new checkpoint state must be created. The state must
inherit from the `StaleEntityCheckpointStateBase` abstract class shown below, and implement each of the abstract methods.
```python
class StaleEntityCheckpointStateBase(CheckpointStateBase, ABC, Generic[Derived]):
    """
    Defines the abstract interface for the checkpoint states that are used for stale entity removal.
    Examples include sql_common state for tracking table and & view urns,
    dbt that tracks node & assertion urns, kafka state tracking topic urns.
    """

    @classmethod
    @abstractmethod
    def get_supported_types(cls) -> List[str]:
        pass

    @abstractmethod
    def add_checkpoint_urn(self, type: str, urn: str) -> None:
        """
        Adds an urn into the list used for tracking the type.
        :param type: The type of the urn such as a 'table', 'view',
         'node', 'topic', 'assertion' that the concrete sub-class understands.
        :param urn: The urn string
        :return: None.
        """
        pass

    @abstractmethod
    def get_urns_not_in(
        self, type: str, other_checkpoint_state: Derived
    ) -> Iterable[str]:
        """
        Gets the urns present in this checkpoint but not the other_checkpoint for the given type.
        :param type: The type of the urn such as a 'table', 'view',
         'node', 'topic', 'assertion' that the concrete sub-class understands.
        :param other_checkpoint_state: the checkpoint state to compute the urn set difference against.
        :return: an iterable to the set of urns present in this checkpoing state but not in the other_checkpoint.
        """
        pass
```

Examples: 
1. [KafkaCheckpointState](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/state/kafka_state.py#L11).
2. [DbtCheckpointState](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/state/dbt_state.py#L16)
3. [BaseSQLAlchemyCheckpointState](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/state/sql_common_state.py#L17)

### 2. Modifying the SourceConfig
The `stateful_ingestion` config param that is mandatory for any source using stateful ingestion needs to be overridden with a custom config that is more specific to the source
and is inherited from `datahub.ingestion.source.state.stale_entity_removal_handler.StatefulStaleMetadataRemovalConfig`. The `StatefulStaleMetadataRemovalConfig` adds the following
additional parameters to the basic stateful ingestion config that is common for all sources. Typical customization involves overriding the `_entity_types` private config member which helps produce
more accurate documentation specific to the source.
```python
import pydantic
from typing import List
from datahub.ingestion.source.state.stateful_ingestion_base import StatefulIngestionConfig
class StatefulStaleMetadataRemovalConfig(StatefulIngestionConfig):
    """ Base specialized config for Stateful Ingestion with stale metadata removal capability. """

    # Allows for sources to define(via override) the entity types they support.
    _entity_types: List[str] = []
    # Whether to enable removal of stale metadata.
    remove_stale_metadata: bool = pydantic.Field(
        default=True,
        description=f"Soft-deletes the entities of type {', '.join(_entity_types)} in the last successful run but missing in the current run with stateful_ingestion enabled.",
    )
```

Examples:
1. The `KafkaSourceConfig`
```python
from typing import List, Optional
import pydantic
from datahub.ingestion.source.state.stale_entity_removal_handler import StatefulStaleMetadataRemovalConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.configuration.source_common import DatasetSourceConfigBase

class KafkaSourceStatefulIngestionConfig(StatefulStaleMetadataRemovalConfig):
    """ Kafka custom stateful ingestion config definition(overrides _entity_types of StatefulStaleMetadataRemovalConfig). """
    _entity_types: List[str] = pydantic.Field(default=["topic"])


class KafkaSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigBase):
    # ...<other config params>...
    """ Override the stateful_ingestion config param with the Kafka custom stateful ingestion config in the KafkaSourceConfig. """
    stateful_ingestion: Optional[KafkaSourceStatefulIngestionConfig] = None
```

2. The [DBTStatefulIngestionConfig](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/dbt.py#L131)
   and the [DBTConfig](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/dbt.py#L317).

### 3. Modifying the SourceReport
The report class of the source should inherit from `StaleEntityRemovalSourceReport` whose definition is shown below.
```python
from typing import List
from dataclasses import dataclass, field
from datahub.ingestion.source.state.stateful_ingestion_base import StatefulIngestionReport
@dataclass
class StaleEntityRemovalSourceReport(StatefulIngestionReport):
    soft_deleted_stale_entities: List[str] = field(default_factory=list)

    def report_stale_entity_soft_deleted(self, urn: str) -> None:
        self.soft_deleted_stale_entities.append(urn)
```

Examples:
1. The `KafkaSourceReport`
```python
from dataclasses import dataclass
from datahub.ingestion.source.state.stale_entity_removal_handler import StaleEntityRemovalSourceReport
@dataclass
class KafkaSourceReport(StaleEntityRemovalSourceReport):
    # <rest of kafka source report specific impl
```
2. [DBTSourceReport](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/dbt.py#L142)
### 4. Modifying the Source
The source must inherit from `StatefulIngestionSourceBase`.

#### 4.1 Instantiate StaleEntityRemovalHandler in the `__init__` method of the source.

Examples:
1. The `KafkaSource`
```python
from datahub.ingestion.source.state.stateful_ingestion_base import StatefulIngestionSourceBase
from datahub.ingestion.source.state.stale_entity_removal_handler import StaleEntityRemovalHandler
class KafkaSource(StatefulIngestionSourceBase):
    def __init__(self, config: KafkaSourceConfig, ctx: PipelineContext):
        # <Rest of KafkaSource initialization>
        # Create and register the stateful ingestion stale entity removal handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.source_config,
            state_type_class=KafkaCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )
```
#### 4.2 Adding entities from current run to the state object.
Use the `add_entity_to_state` method of the `StaleEntityRemovalHandler`.

Examples:
```python
# Kafka
self.stale_entity_removal_handler.add_entity_to_state(
    type="topic",
    urn=topic_urn,)

# DBT
self.stale_entity_removal_handler.add_entity_to_state(
    type="dataset",
    urn=node_datahub_urn
)
self.stale_entity_removal_handler.add_entity_to_state(
    type="assertion",
    urn=node_datahub_urn,
)
```

#### 4.3 Emitting soft-delete workunits associated with the stale entities.
```python
def get_workunits(self) -> Iterable[MetadataWorkUnit]:
    #
    # Emit the rest of the workunits for the source.
    # NOTE: Populating the current state happens during the execution of this code.
    # ...
    
    # Clean up stale entities at the end
    yield from self.stale_entity_removal_handler.gen_removed_entity_workunits()
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
            cur_start_time_millis=datetime_to_ts_millis(self.config.start_time)
        ):
            return
        
        # Generate the workunits.
        # <code for generating the workunits>
        # Update checkpoint state for this run.
        self.redundant_run_skip_handler.update_state(
            start_time_millis=datetime_to_ts_millis(self.config.start_time),
            end_time_millis=datetime_to_ts_millis(self.config.end_time),
        )
```