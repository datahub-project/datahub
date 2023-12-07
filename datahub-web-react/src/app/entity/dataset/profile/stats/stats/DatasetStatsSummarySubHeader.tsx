import React from 'react';
import { DatasetStatsSummary as DatasetStatsSummaryObj } from '../../../../../../types.generated';
import { useBaseEntity } from '../../../../shared/EntityContext';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { DatasetStatsSummary } from '../../../shared/DatasetStatsSummary';
import { getLastUpdatedMs } from '../../../shared/utils';

export const DatasetStatsSummarySubHeader = () => {
    const result = useBaseEntity<GetDatasetQuery>();
    const dataset = result?.dataset;

    const maybeStatsSummary = dataset?.statsSummary as DatasetStatsSummaryObj;

    const maybeLastProfile =
        dataset?.datasetProfiles && dataset.datasetProfiles.length ? dataset.datasetProfiles[0] : undefined;

    const rowCount = maybeLastProfile?.rowCount;
    const columnCount = maybeLastProfile?.columnCount;
    const sizeInBytes = maybeLastProfile?.sizeInBytes;
    const totalSqlQueries = dataset?.usageStats?.aggregations?.totalSqlQueries;
    const queryCountLast30Days = maybeStatsSummary?.queryCountLast30Days;
    const uniqueUserCountLast30Days = maybeStatsSummary?.uniqueUserCountLast30Days;
    const lastUpdatedMs = getLastUpdatedMs(dataset?.properties, dataset?.operations);

    return (
        <DatasetStatsSummary
            rowCount={rowCount}
            columnCount={columnCount}
            sizeInBytes={sizeInBytes}
            totalSqlQueries={totalSqlQueries}
            queryCountLast30Days={queryCountLast30Days}
            uniqueUserCountLast30Days={uniqueUserCountLast30Days}
            lastUpdatedMs={lastUpdatedMs}
        />
    );
};
