import { ChartProperties } from '../../../../../../../../types.generated';

// This util is used for Charts and Dashboards who get last updated differently than others
export function getLastUpdatedMs(
    properties: Pick<ChartProperties, 'lastModified' | 'lastRefreshed'> | null | undefined,
): number | undefined {
    return properties?.lastRefreshed || properties?.lastModified?.time || undefined;
}
