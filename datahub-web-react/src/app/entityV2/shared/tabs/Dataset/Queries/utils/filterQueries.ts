/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Query } from '@app/entityV2/shared/tabs/Dataset/Queries/types';

import { FacetFilterInput } from '@types';

/**
 * Filter queries by a search string. Compares name, description, and query statement.
 *
 * @param filterText the search text
 * @param queries the queries to filter
 */
export const filterQueries = (filterText, queries: Query[]) => {
    const lowerFilterText = filterText.toLowerCase();
    return queries.filter((query) => {
        return (
            query.title?.toLowerCase().includes(lowerFilterText) ||
            query.description?.toLowerCase().includes(lowerFilterText) ||
            query.query?.toLowerCase()?.includes(lowerFilterText)
        );
    });
};

export const getQueryEntitiesFilter = (entityUrn?: string, siblingUrn?: string) => {
    const values = siblingUrn ? [entityUrn as string, siblingUrn] : [entityUrn as string];
    return { field: 'entities', values };
};

export const getAndFilters = (
    selectedColumnsFilter: FacetFilterInput,
    selectedUsersFilter: FacetFilterInput,
    existingFilters: FacetFilterInput[] = [],
) => {
    let andFilters = selectedColumnsFilter.values?.length
        ? [...existingFilters, selectedColumnsFilter]
        : existingFilters;
    andFilters = selectedUsersFilter.values?.length ? [...andFilters, selectedUsersFilter] : andFilters;
    return andFilters;
};
