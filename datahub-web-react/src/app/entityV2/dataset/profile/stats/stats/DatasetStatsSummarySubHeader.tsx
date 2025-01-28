import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { DatasetStatsSummary as DatasetStatsSummaryObj, EntityType } from '../../../../../../types.generated';
import { useBaseEntity } from '../../../../../entity/shared/EntityContext';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { DatasetStatsSummary } from '../../../shared/DatasetStatsSummary';

export const DatasetStatsSummarySubHeader = () => {
    const result = useBaseEntity<GetDatasetQuery>();
    const dataset = result?.dataset;

    const maybeStatsSummary = dataset?.statsSummary as DatasetStatsSummaryObj;

    const latestFullTableProfile = dataset?.latestFullTableProfile?.[0];
    const latestPartitionProfile = dataset?.latestPartitionProfile?.[0];

    const maybeLastProfile = latestFullTableProfile || latestPartitionProfile || undefined;

    const maybeLastOperation = dataset?.operations && dataset.operations.length ? dataset.operations[0] : undefined;

    const rowCount = maybeLastProfile?.rowCount;
    const columnCount = maybeLastProfile?.columnCount;
    const sizeInBytes = maybeLastProfile?.sizeInBytes;
    const totalSqlQueries = dataset?.usageStats?.aggregations?.totalSqlQueries;
    const queryCountLast30Days = maybeStatsSummary?.queryCountLast30Days;
    const uniqueUserCountLast30Days = maybeStatsSummary?.uniqueUserCountLast30Days;

    const lastUpdatedMs = maybeLastOperation?.lastUpdatedTimestamp;

    const entityRegistry = useEntityRegistry();
    const platformName = dataset?.platform && entityRegistry.getDisplayName(EntityType.DataPlatform, dataset?.platform);
    const platformLogoUrl = dataset?.platform?.properties?.logoUrl;

    return (
        <DatasetStatsSummary
            rowCount={rowCount}
            columnCount={columnCount}
            sizeInBytes={sizeInBytes}
            totalSqlQueries={totalSqlQueries}
            queryCountLast30Days={queryCountLast30Days}
            uniqueUserCountLast30Days={uniqueUserCountLast30Days}
            lastUpdatedMs={lastUpdatedMs}
            platformName={platformName}
            platformLogoUrl={platformLogoUrl}
            subTypes={dataset?.subTypes?.typeNames || undefined}
        />
    );
};
