import abc
import logging
from datetime import datetime
from typing import Any, Dict, Iterable

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.usage.usage_common import GenericAggregatedDataset

logger = logging.getLogger(__name__)


class SqlUsageSource(Source, abc.ABC):
    @abc.abstractmethod
    def get_config(self):
        pass

    @abc.abstractmethod
    def get_platform(self) -> str:
        pass

    @abc.abstractmethod
    def get_history(self) -> Iterable:
        pass

    @abc.abstractmethod
    def aggregate_events(
        self, events: Iterable
    ) -> Dict[datetime, Dict[Any, GenericAggregatedDataset]]:
        pass

    @abc.abstractmethod
    def get_report(self) -> SourceReport:
        pass

    def sql_compatibility_change(self, row):
        # Make some minor type conversions.
        if hasattr(row, "_asdict"):
            # Compat with SQLAlchemy 1.3 and 1.4
            # See https://docs.sqlalchemy.org/en/14/changelog/migration_14.html#rowproxy-is-no-longer-a-proxy-is-now-called-row-and-behaves-like-an-enhanced-named-tuple.
            event_dict = row._asdict()
        else:
            event_dict = dict(row)

        return event_dict

    def make_sql_engine(self) -> Engine:
        url = self.get_config().get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.get_config().options)
        return engine

    def _make_usage_stat(self, agg: GenericAggregatedDataset) -> MetadataWorkUnit:
        return agg.make_usage_workunit(
            self.get_config().bucket_duration,
            lambda resource: builder.make_dataset_urn(
                self.get_platform(), resource.lower(), self.get_config().env
            ),
            self.get_config().top_n_queries,
        )

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        events = self.get_history()
        aggregated_info = self.aggregate_events(events)

        for time_bucket in aggregated_info.values():
            for aggregate in time_bucket.values():
                wu = self._make_usage_stat(aggregate)
                self.get_report().report_workunit(wu)
                yield wu

    def close(self):
        pass
