/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import { BaseDatum, YAccessor } from '@components/components/BarChart/types';

export default function useMinDataValue(data: BaseDatum[], yAccessor: YAccessor): number {
    return useMemo(() => Math.min(...data.map(yAccessor)) ?? 0, [data, yAccessor]);
}
