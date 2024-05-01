import { DatasetProperties, ChartProperties, Operation } from '../../../../types.generated';

// Dataset 
export type LastUpdatedMs = {
    property: 'lastModified' | 'lastUpdated' | undefined;
    lastUpdatedMs: number | undefined
};
export function getLastUpdatedMs(
    properties: Pick<DatasetProperties, 'lastModified'> | null | undefined,
    operations: Pick<Operation, 'lastUpdatedTimestamp'>[] | null | undefined,
): LastUpdatedMs {
    const lastModified = properties?.lastModified?.time || 0;
    const lastUpdated = (operations?.length && operations[0].lastUpdatedTimestamp) || 0;

    const max = Math.max(lastModified, lastUpdated);

    if (max === 0) return ({ property: undefined, lastUpdatedMs: undefined });
    if (max === lastModified) return ({ property: 'lastModified', lastUpdatedMs: lastModified });
    return ({ property: 'lastUpdated', lastUpdatedMs: lastUpdated });
}

// Chart & Dashboard
export type LastChartUpdatedMs = {
    property: 'lastModified' | 'lastRefreshed' | undefined;
    lastUpdatedMs: number | undefined;
};

export function getLastChartUpdatedMs(
    properties: Pick<ChartProperties, 'lastModified' | 'lastRefreshed'> | null | undefined,
): LastChartUpdatedMs {
    const lastModified = properties?.lastModified?.time || 0;
    const lastRefreshed = properties?.lastRefreshed || 0;

    const max = Math.max(lastModified, lastRefreshed);

    if (max === 0) return ({ property: undefined, lastUpdatedMs: undefined });
    if (max === lastModified) return ({ property: 'lastModified', lastUpdatedMs: lastModified });
    return ({ property: 'lastRefreshed', lastUpdatedMs: lastRefreshed });
}