from typing import Callable, Dict, List, Optional, Union

from acryl_datahub_cloud.metadata.schema_classes import UsageFeaturesClass
from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import KafkaAuditHeaderClass, SystemMetadataClass


class UsageFeaturePatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        """Initializes a MonitorPatchBuilder instance.

        Args:
            urn: The URN of the monitor.
            system_metadata: The system metadata of the data job (optional).
            audit_header: The Kafka audit header of the data job (optional).
        """
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )

    def setQueryCountLast30Days(
        self, count: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the query count for the last 30 days.

        Args:
            count: The query count for the last 30 days.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("queryCountLast30Days",),
            value=count,
        )
        return self

    def setViewCountLast30Days(
        self, count: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the view count for the last 30 days.

        Args:
            count: The view count for the last 30 days.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("viewCountLast30Days",),
            value=count,
        )
        return self

    def setViewCountTotal(self, count: Optional[int]) -> "UsageFeaturePatchBuilder":
        """Sets the total view count.

        Args:
            count: The total view count.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("viewCountTotal",),
            value=count,
        )
        return self

    def setViewCountPercentileLast30Days(
        self, count: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the view count percentile for the last 30 days.

        Args:
            count: The view count percentile for the last 30 days.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("viewCountPercentileLast30Days",),
            value=count,
        )
        return self

    def setUsageCountLast30Days(
        self, count: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the usage count for the last 30 days.

        Args:
            count: The usage count for the last 30 days.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("usageCountLast30Days",),
            value=count,
        )
        return self

    def setUniqueUserCountLast30Days(
        self, count: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the unique user count for the last 30 days.

        Args:
            count: The unique user count for the last 30 days.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("uniqueUserCountLast30Days",),
            value=count,
        )
        return self

    def setWriteCountLast30Days(
        self, count: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the write count for the last 30 days.

        Args:
            count: The write count for the last 30 days.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("writeCountLast30Days",),
            value=count,
        )
        return self

    def setQueryCountPercentileLast30Days(
        self, count: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the query count percentile for the last 30 days.

        Args:
            count: The query count percentile for the last 30 days.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("queryCountPercentileLast30Days",),
            value=count,
        )
        return self

    def setQueryCountRankLast30Days(
        self, count: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the query count rank for the last 30 days.

        Args:
            count: The query count rank for the last 30 days.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("queryCountRankLast30Days",),
            value=count,
        )
        return self

    def setUniqueUserPercentileLast30Days(
        self, count: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the unique user percentile for the last 30 days.

        Args:
            count: The unique user percentile for the last 30 days.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("uniqueUserPercentileLast30Days",),
            value=count,
        )
        return self

    def setUniqueUserRankLast30Days(
        self, count: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the unique user rank for the last 30 days.

        Args:
            count: The unique user rank for the last 30 days.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("uniqueUserRankLast30Days",),
            value=count,
        )
        return self

    def setWriteCountPercentileLast30Days(
        self, count: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the write count percentile for the last 30 days.

        Args:
            count: The write count percentile for the last 30 days.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("writeCountPercentileLast30Days",),
            value=count,
        )
        return self

    def setWriteCountRankLast30Days(
        self, count: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the write count rank for the last 30 days.

        Args:
            count: The write count rank for the last 30 days.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("writeCountRankLast30Days",),
            value=count,
        )
        return self

    def setTopUsersLast30Days(
        self, users: Optional[List[str]]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the top users for the last 30 days.

        Args:
            users: The list of top users for the last 30 days.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("topUsersLast30Days",),
            value=users,
        )
        return self

    def setSizeInBytesPercentile(
        self, percentile: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the size in bytes percentile.

        Args:
            percentile: The size in bytes percentile.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("sizeInBytesPercentile",),
            value=percentile,
        )
        return self

    def setSizeInBytesRank(self, rank: Optional[int]) -> "UsageFeaturePatchBuilder":
        """Sets the size in bytes rank.

        Args:
            rank: The size in bytes rank.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("sizeInBytesRank",),
            value=rank,
        )
        return self

    def setRowCountPercentile(
        self, percentile: Optional[int]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the row count percentile.

        Args:
            percentile: The row count percentile.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("rowCountPercentile",),
            value=percentile,
        )
        return self

    def setUsageSearchScoreMultiplier(
        self, multiplier: Optional[float]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the usage search score multiplier.

        Args:
            multiplier: The usage search score multiplier.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("usageSearchScoreMultiplier",),
            value=multiplier,
        )
        return self

    def setUsageFreshnessScoreMultiplier(
        self, multiplier: Optional[float]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the usage freshness score multiplier.

        Args:
            multiplier: The usage freshness score multiplier.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("usageFreshnessScoreMultiplier",),
            value=multiplier,
        )
        return self

    def setCustomDatahubScoreMultiplier(
        self, multiplier: Optional[float]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the custom DataHub score multiplier.

        Args:
            multiplier: The custom DataHub score multiplier.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("customDatahubScoreMultiplier",),
            value=multiplier,
        )
        return self

    def setCombinedSearchRankingMultiplier(
        self, multiplier: Optional[float]
    ) -> "UsageFeaturePatchBuilder":
        """Sets the combined search ranking multiplier.

        Args:
            multiplier: The combined search ranking multiplier.

        Returns:
            UsageFeaturePatchBuilder: The instance of the builder.
        """
        self._add_patch(
            UsageFeaturesClass.ASPECT_NAME,
            "add",
            path=("combinedSearchRankingMultiplier",),
            value=multiplier,
        )
        return self

    def apply_usage_features(
        self, usage_features: UsageFeaturesClass
    ) -> "UsageFeaturePatchBuilder":
        """
        Applies the usage features to the patch builder.
        :param usage_features: UsageFeaturesClass object containing the features to apply
        :return: self (UsageFeaturePatchBuilder instance)
        """
        feature_setters: Dict[
            str,
            Union[
                Callable[[Optional[int]], UsageFeaturePatchBuilder],
                Callable[[Optional[float]], UsageFeaturePatchBuilder],
                Callable[[Optional[List[str]]], UsageFeaturePatchBuilder],
            ],
        ] = {
            "queryCountLast30Days": self.setQueryCountLast30Days,
            "viewCountLast30Days": self.setViewCountLast30Days,
            "viewCountTotal": self.setViewCountTotal,
            "viewCountPercentileLast30Days": self.setViewCountPercentileLast30Days,
            "usageCountLast30Days": self.setUsageCountLast30Days,
            "uniqueUserCountLast30Days": self.setUniqueUserCountLast30Days,
            "writeCountLast30Days": self.setWriteCountLast30Days,
            "queryCountPercentileLast30Days": self.setQueryCountPercentileLast30Days,
            "queryCountRankLast30Days": self.setQueryCountRankLast30Days,
            "uniqueUserPercentileLast30Days": self.setUniqueUserPercentileLast30Days,
            "uniqueUserRankLast30Days": self.setUniqueUserRankLast30Days,
            "writeCountPercentileLast30Days": self.setWriteCountPercentileLast30Days,
            "writeCountRankLast30Days": self.setWriteCountRankLast30Days,
            "topUsersLast30Days": self.setTopUsersLast30Days,
            "sizeInBytesPercentile": self.setSizeInBytesPercentile,
            "sizeInBytesRank": self.setSizeInBytesRank,
            "rowCountPercentile": self.setRowCountPercentile,
            "usageSearchScoreMultiplier": self.setUsageSearchScoreMultiplier,
            "usageFreshnessScoreMultiplier": self.setUsageFreshnessScoreMultiplier,
            "customDatahubScoreMultiplier": self.setCustomDatahubScoreMultiplier,
            "combinedSearchRankingMultiplier": self.setCombinedSearchRankingMultiplier,
        }

        for feature, setter in feature_setters.items():
            value = getattr(usage_features, feature)
            if value is not None:
                assert value is not None
                setter(value)

        return self
