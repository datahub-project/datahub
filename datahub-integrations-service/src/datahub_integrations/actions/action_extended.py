from abc import ABC, abstractmethod
from typing import Callable, Dict, Generic, Iterable, List, Optional, TypeVar

from datahub.configuration.common import ConfigModel
from datahub.ingestion.graph.client import DataHubGraph
from datahub.secret.datahub_secret_store import (
    DataHubSecretStore,
    DataHubSecretStoreConfig,
)
from datahub.secret.secret_common import resolve_recipe
from datahub.secret.secret_store import SecretStore
from datahub_actions.pipeline.pipeline_context import PipelineContext
from loguru import logger
from pydantic import BaseModel, Field
from ratelimit import limits, sleep_and_retry

from datahub_integrations.actions.oss.stats_util import (
    ActionStageReport,
    ReportingAction,
)
from datahub_integrations.actions.secret_store.environment_secret_store import (
    EnvironmentSecretStore,
)
from datahub_integrations.actions.stats_util import Stage

T = TypeVar("T")


def create_rate_limited_function(
    rate_limit: int, time_period: int, func: Callable
) -> Callable:
    @sleep_and_retry
    @limits(calls=rate_limit, period=time_period)
    def rate_limited_func(*args, **kwargs):  # type: ignore
        return func(*args, **kwargs)

    return rate_limited_func


class ExtendedActionStats(BaseModel):
    stage_processing_stats: Optional[Dict[Stage, ActionStageReport]] = Field(
        {},
        description="Stage specific processing stats for the action.",
    )


class AutomationActionConfig(ConfigModel):
    event_processing_rate_limit: int = Field(
        default=10,
        description="Rate limit for processing events. Default is 10 event per rate period.",
    )

    event_processing_rate_period: int = Field(
        default=1,
        description="Rate limit period for processing events. Default is 1 second.",
    )


class ExtendedAction(ReportingAction, Generic[T], ABC):
    """
    A wrapper interface that adds additional methods like bootstrap and rollback
    to the Action class.
    It forces the implementer to implement specific methods that are required
    for bootstrapping and rolling back the action.
    """

    def __init__(self, config: AutomationActionConfig, ctx: PipelineContext):
        super().__init__(ctx)

        self._stats = ActionStageReport()

        self.config = config
        self._stats.start()

    @abstractmethod
    def rollbackable_assets(self) -> Iterable[T]:
        """Return an iterable of assets to execute rollback on."""
        pass

    @abstractmethod
    def rollback_asset(self, asset: T) -> None:
        """Rollback an individual asset."""
        pass

    def get_report(self) -> ActionStageReport:
        """Get the action report."""
        return self._stats

    @abstractmethod
    def bootstrappable_assets(self) -> Iterable[T]:
        """Return an iterable of assets to execute bootstrap on."""
        pass

    @abstractmethod
    def bootstrap_asset(self, asset: T) -> None:
        """Bootstrap an individual asset."""
        pass

    def get_action_urn(self) -> str:
        """Get the action URN."""
        return self.action_urn

    def bootstrap(self) -> None:
        """Bootstrap the action."""
        bootstrap_report = self._stats = ActionStageReport()
        bootstrap_report.start()

        success = True
        try:
            # start a thread to bootstrap the action
            # while bootstrap is running, the action will continue processing any
            # real time events
            # start a threadpool executor
            # Create a ThreadPoolExecutor
            import concurrent.futures

            # Create a rate-limited version of the function
            rate_limit = self.config.event_processing_rate_limit
            time_period = self.config.event_processing_rate_period
            rate_limited_bootstrap_asset = create_rate_limited_function(
                rate_limit, time_period, self.bootstrap_asset
            )

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                bootstrappable_assets = self.bootstrappable_assets()
                futures = [
                    executor.submit(
                        rate_limited_bootstrap_asset,
                        asset,
                    )
                    for asset in bootstrappable_assets
                ]
                bootstrap_report.total_assets_to_process = len(futures)
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Error bootstrapping dataset: {e}", exc_info=True)
                        success = False
        except Exception as e:
            logger.error(f"Error bootstrapping action: {e}", exc_info=True)
            success = False

        self.bootstrapping = False
        bootstrap_report.end(success=success)
        logger.info("Bootstrap Report: {}".format(bootstrap_report))

    def rollback(self) -> None:
        """Rollback the action."""
        rollback_report = self._stats = ActionStageReport()
        rollback_report.start()

        success = True
        try:
            # start a thread to rollback the action
            # while rollback is running, the action will continue processing any
            # real time events
            # start a threadpool executor
            # Create a ThreadPoolExecutor
            import concurrent.futures

            # Create a rate-limited version of the function
            rate_limit = self.config.event_processing_rate_limit
            time_period = self.config.event_processing_rate_period
            rate_limited_rollback_asset = create_rate_limited_function(
                rate_limit, time_period, self.rollback_asset
            )

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                rollbackable_assets = self.rollbackable_assets()
                futures = [
                    executor.submit(rate_limited_rollback_asset, asset)
                    for asset in rollbackable_assets
                ]
                rollback_report.total_assets_to_process = len(futures)
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Error rolling back dataset: {e}")
                        success = False
        except Exception as e:
            logger.error(f"Error rolling back action: {e}")
            success = False
        rollback_report.end(success=success)

    @staticmethod
    def resolve_secrets(config_str: str, graph: DataHubGraph) -> Dict:
        logger.info("Resolving secrets")
        secret_stores: List[SecretStore] = [
            DataHubSecretStore(
                DataHubSecretStoreConfig(
                    graph_client=graph,
                )
            ),
            EnvironmentSecretStore(
                config={},
            ),
        ]

        resolved_recipe = resolve_recipe(config_str, secret_stores)

        return resolved_recipe
