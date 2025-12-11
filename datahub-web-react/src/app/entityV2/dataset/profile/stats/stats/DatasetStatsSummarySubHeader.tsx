/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { DatasetStatsSummary } from '@app/entityV2/dataset/shared/DatasetStatsSummary';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { DatasetStatsSummary as DatasetStatsSummaryObj, EntityType } from '@types';

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
