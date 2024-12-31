import { DatasetProperties, Operation } from '../../../../types.generated';

export function getLastUpdatedMs(
    properties: Pick<DatasetProperties, 'lastModified'> | null | undefined,
    operations: Pick<Operation, 'lastUpdatedTimestamp'>[] | null | undefined,
): number | undefined {
    return (
        Math.max(
            properties?.lastModified?.time || 0,
            (operations?.length && operations[0].lastUpdatedTimestamp) || 0,
        ) || undefined
    );
}
