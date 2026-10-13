import { mergeUrnEntities } from '@app/sharedV2/utils/mergeUrnEntities';

type UrnEntity = {
    urn: string;
};

export type MetricsSidebarPaginationState<T extends UrnEntity> = {
    criteriaKey: string;
    scrollId: string | null;
    entities: T[];
};

export function createMetricsSidebarPaginationState<T extends UrnEntity>(
    criteriaKey: string,
): MetricsSidebarPaginationState<T> {
    return { criteriaKey, scrollId: null, entities: [] };
}

export function getMetricsSidebarPaginationView<T extends UrnEntity>(
    state: MetricsSidebarPaginationState<T>,
    criteriaKey: string,
): Pick<MetricsSidebarPaginationState<T>, 'scrollId' | 'entities'> {
    if (state.criteriaKey === criteriaKey) {
        return state;
    }
    return { scrollId: null, entities: [] };
}

/**
 * Replaces the first page and merges later pages by URN. Existing rows are
 * refreshed when the server returns them again, while new rows retain server order.
 */
export function mergeMetricsSidebarPage<T extends UrnEntity>(current: T[], fresh: T[], isFirstPage: boolean): T[] {
    return mergeUrnEntities(current, fresh, isFirstPage);
}

export function mergeMetricsSidebarPaginationPage<T extends UrnEntity>(
    state: MetricsSidebarPaginationState<T>,
    criteriaKey: string,
    fresh: T[],
): MetricsSidebarPaginationState<T> {
    const isCurrentCriteria = state.criteriaKey === criteriaKey;
    const current = isCurrentCriteria ? state.entities : [];
    const entities = mergeMetricsSidebarPage(current, fresh, !isCurrentCriteria || state.scrollId === null);

    if (isCurrentCriteria && entities === state.entities) return state;
    return {
        criteriaKey,
        scrollId: isCurrentCriteria ? state.scrollId : null,
        entities,
    };
}

export function advanceMetricsSidebarPagination<T extends UrnEntity>(
    state: MetricsSidebarPaginationState<T>,
    criteriaKey: string,
    scrollId: string,
): MetricsSidebarPaginationState<T> {
    const entities = state.criteriaKey === criteriaKey ? state.entities : [];
    if (state.criteriaKey === criteriaKey && state.scrollId === scrollId) return state;
    return { criteriaKey, scrollId, entities };
}
