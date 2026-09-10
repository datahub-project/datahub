import { useState } from 'react';

import {
    MetricsSidebarPaginationState,
    createMetricsSidebarPaginationState,
    getMetricsSidebarPaginationView,
} from '@app/metrics/utils/metricsSidebarPagination';

type UrnEntity = {
    urn: string;
};

/**
 * Keeps sidebar scroll state aligned with the current query criteria.
 * Reset happens during render so A → B → A cannot reuse A's old cursor.
 */
export default function useMetricsSidebarPagination<T extends UrnEntity>(criteriaKey: string) {
    const [pagination, setPagination] = useState(() => createMetricsSidebarPaginationState<T>(criteriaKey));
    if (pagination.criteriaKey !== criteriaKey) {
        setPagination(createMetricsSidebarPaginationState<T>(criteriaKey));
    }

    return {
        ...getMetricsSidebarPaginationView(pagination, criteriaKey),
        setPagination,
    };
}

export type { MetricsSidebarPaginationState };
