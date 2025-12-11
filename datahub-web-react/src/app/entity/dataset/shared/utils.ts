/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DatasetProperties, Operation } from '@types';

export function getLastUpdatedMs(
    properties: Pick<DatasetProperties, 'lastModified'> | null | undefined,
    operations: Pick<Operation, 'lastUpdatedTimestamp'>[] | null | undefined,
): number | undefined {
    return (
        Math?.max(
            properties?.lastModified?.time || 0,
            (operations?.length && operations[0].lastUpdatedTimestamp) || 0,
        ) || undefined
    );
}
