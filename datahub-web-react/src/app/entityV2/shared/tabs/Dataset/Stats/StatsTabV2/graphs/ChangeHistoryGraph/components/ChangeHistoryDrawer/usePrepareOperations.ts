/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import { OPERATIONS_LIMIT } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/constants';
import { Operation } from '@src/types.generated';

export default function usePrepareOperations(operations: Operation[]) {
    return useMemo(() => {
        const seenKeys = new Set<string>();
        const uniqueOperations = operations.filter((operation) => {
            const key = `${operation.lastUpdatedTimestamp}-${operation.operationType}-${
                operation.customOperationType || ''
            }`;
            return seenKeys.has(key) ? false : (seenKeys.add(key), true);
        });

        return uniqueOperations.slice(0, OPERATIONS_LIMIT);
    }, [operations]);
}
