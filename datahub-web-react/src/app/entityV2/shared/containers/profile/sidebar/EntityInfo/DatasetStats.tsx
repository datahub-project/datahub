import React from 'react';

import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { getDatasetLastUpdatedMs } from '@app/entityV2/shared/utils';
import { DatasetStatsSummary } from '@app/sharedV2/datasetStats/DatasetStatsSummary';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { DatasetStatsSummary as DatasetStatsSummaryObj } from '@types';

export default function DatasetStats() {
    const { entityData } = useEntityData();
    const dataset = entityData as GetDatasetQuery['dataset'];

    const maybeStatsSummary = dataset?.statsSummary as DatasetStatsSummaryObj;
    const maybeLastProfile = dataset?.latestFullTableProfile?.[0] || dataset?.latestPartitionProfile?.[0] || undefined;

    return (
        <DatasetStatsSummary
            rowCount={maybeLastProfile?.rowCount}
            columnCount={maybeLastProfile?.columnCount}
            sizeInBytes={maybeLastProfile?.sizeInBytes}
            totalSqlQueries={dataset?.usageStats?.aggregations?.totalSqlQueries}
            queryCountLast30Days={maybeStatsSummary?.queryCountLast30Days}
            uniqueUserCountLast30Days={maybeStatsSummary?.uniqueUserCountLast30Days}
            lastUpdatedMs={getDatasetLastUpdatedMs(dataset?.properties, dataset?.operations).lastUpdatedMs}
            shouldWrap
        />
    );
}
