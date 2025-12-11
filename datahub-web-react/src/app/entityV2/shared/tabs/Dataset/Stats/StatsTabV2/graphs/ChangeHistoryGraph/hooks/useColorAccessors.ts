/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import {
    AnyOperationType,
    OperationsData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { createColorAccessors } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/utils';
import { CalendarData } from '@src/alchemy-components/components/CalendarChart/types';

export default function useColorAccessors(
    summary: OperationsData | undefined,
    data: CalendarData<OperationsData>[],
    types: AnyOperationType[],
) {
    return useMemo(() => createColorAccessors(summary, data, types), [summary, data, types]);
}
