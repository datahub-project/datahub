/**
 * React hook for managing analytics chart colors
 *
 * Provides color assignment, manual overrides, and statistics
 * for DataHub analytics charts
 */
import { useCallback, useMemo, useState } from 'react';

import {
    ColorAssignmentResult,
    SeriesColorConfig,
    assignAnalyticsChartColors,
    getColorAssignmentStats,
} from '@app/analyticsDashboardV2/utils/analyticsChartColors';

interface UseAnalyticsChartColorsOptions {
    persistenceKey?: string;
    initialOverrides?: Record<string, string>;
}

interface UseAnalyticsChartColorsReturn {
    colorAssignments: Record<string, SeriesColorConfig>;
    getColorByKey: (key: string) => string;
    updateColor: (key: string, color: string) => void;
    resetColor: (key: string) => void;
    assignmentStats: {
        total: number;
        entityMatched: number;
        qualitative: number;
        generated: number;
        userOverrides: number;
    };
}

/**
 * Hook for managing analytics chart colors with smart assignment
 *
 * Features:
 * - Automatic color assignment based on entity types
 * - Manual color overrides
 * - Assignment statistics
 * - Memoized for performance
 *
 * @param seriesKeys - Array of series identifiers
 * @param options - Configuration options
 * @returns Color management interface
 */
export function useAnalyticsChartColors(
    seriesKeys: string[],
    options: UseAnalyticsChartColorsOptions = {},
): UseAnalyticsChartColorsReturn {
    const [overrides, setOverrides] = useState<Record<string, string>>(options.initialOverrides || {});

    const colorResult: ColorAssignmentResult = useMemo(
        () => assignAnalyticsChartColors(seriesKeys, overrides),
        [seriesKeys, overrides],
    );

    const updateColor = useCallback((key: string, color: string) => {
        setOverrides((prev) => ({ ...prev, [key]: color }));
    }, []);

    const resetColor = useCallback((key: string) => {
        setOverrides((prev) => {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { [key]: removed, ...rest } = prev;
            return rest;
        });
    }, []);

    const getColorByKey = useCallback((key: string) => colorResult.assignments[key]?.color || '#9CA3AF', [colorResult]);

    const assignmentStats = useMemo(() => getColorAssignmentStats(colorResult), [colorResult]);

    return {
        colorAssignments: colorResult.assignments,
        getColorByKey,
        updateColor,
        resetColor,
        assignmentStats,
    };
}
