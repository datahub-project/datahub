/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useDndContext } from '@dnd-kit/core';
import { useMemo } from 'react';

import { ModulePositionInput } from '@app/homeV3/template/types';

/**
 * Optimized hook to determine if we're dragging from a specific row
 * with minimal re-renders
 */
export function useDragRowContext(rowIndex: number): boolean {
    const { active } = useDndContext();

    return useMemo(() => {
        const activeDragData = active?.data?.current as { position?: ModulePositionInput } | undefined;
        return activeDragData?.position?.rowIndex === rowIndex;
    }, [active?.data, rowIndex]);
}
