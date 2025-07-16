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
    }, [active?.data?.current, rowIndex]);
} 