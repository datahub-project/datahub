import React from 'react';
import { DatasetStatsSummary as DatasetStatsSummaryObj, EntityType } from '../../../../../../types.generated';
import { useBaseEntity } from '../../../../../entity/shared/EntityContext';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { DatasetStatsSummary } from '../../../shared/DatasetStatsSummary';
import { useEntityRegistry } from '../../../../../useEntityRegistry';

export const DatasetStatsSummarySubHeader = () => {
    const result = useBaseEntity<GetDatasetQuery>();
    const dataset = result?.dataset;

    const maybeStatsSummary = dataset?.statsSummary as DatasetStatsSummaryObj;

    const maybeLastProfile =
        dataset?.datasetProfiles && dataset.datasetProfiles.length ? dataset.datasetProfiles[0] : undefined;

    const maybeLastOperation = dataset?.operations && dataset.operations.length ? dataset.operations[0] : undefined;

    const rowCount = maybeLastProfile?.rowCount;
    const columnCount = maybeLastProfile?.columnCount;
    const sizeInBytes = maybeLastProfile?.sizeInBytes;
    const totalSqlQueries = dataset?.usageStats?.aggregations?.totalSqlQueries;
    const queryCountLast30Days = maybeStatsSummary?.queryCountLast30Days;
    const queryCountPercentileLast30Days = maybeStatsSummary?.queryCountPercentileLast30Days;
    const uniqueUserCountLast30Days = maybeStatsSummary?.uniqueUserCountLast30Days;
    const uniqueUserPercentileLast30Days = maybeStatsSummary?.uniqueUserPercentileLast30Days;

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
            queryCountPercentileLast30Days={queryCountPercentileLast30Days}
            uniqueUserCountLast30Days={uniqueUserCountLast30Days}
            uniqueUserPercentileLast30Days={uniqueUserPercentileLast30Days}
            lastUpdatedMs={lastUpdatedMs}
            platformName={platformName}
            platformLogoUrl={platformLogoUrl}
            subTypes={dataset?.subTypes?.typeNames || undefined}
        />
    );
};
