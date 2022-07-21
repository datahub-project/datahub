import React from 'react';
import {
    DatasetStatsSummary as DatasetStatsSummaryObj,
    DatasetProfile,
    Operation,
} from '../../../../../../types.generated';
import { useBaseEntity } from '../../../../shared/EntityContext';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { DatasetStatsSummary } from '../../../shared/DatasetStatsSummary';

export const DatasetStatsSummarySubHeader = () => {
    const result = useBaseEntity<GetDatasetQuery>();
    const dataset = result?.dataset;

    const maybeStatsSummary = dataset?.statsSummary as DatasetStatsSummaryObj;
    const maybeLastProfile =
        ((dataset?.datasetProfiles?.length || 0) > 0 && (dataset?.datasetProfiles![0] as DatasetProfile)) || undefined;
    const maybeLastOperation =
        ((dataset?.operations?.length || 0) > 0 && (dataset?.operations![0] as Operation)) || undefined;

    const rowCount = maybeLastProfile?.rowCount;
    const queryCountLast30Days = maybeStatsSummary?.queryCountLast30Days;
    const uniqueUserCountLast30Days = maybeStatsSummary?.uniqueUserCountLast30Days;
    const lastUpdatedMs = maybeLastOperation?.lastUpdatedTimestamp;

    return (
        <DatasetStatsSummary
            rowCount={rowCount}
            queryCountLast30Days={queryCountLast30Days}
            uniqueUserCountLast30Days={uniqueUserCountLast30Days}
            lastUpdatedMs={lastUpdatedMs}
        />
    );
};
