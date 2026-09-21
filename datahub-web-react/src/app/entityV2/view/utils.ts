import { ViewBuilderState } from '@app/entityV2/view/types';
import { LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { convertLogicalPredicateToOrFilters } from '@app/sharedV2/queryBuilder/builder/utils';

import { DataHubView, DataHubViewType, EntityType, LogicalOperator } from '@types';

/**
 *  The max single-page results in both the View Select and Manage Views list.
 *
 *  The explicit assumption is that the number of public + personal views for a user
 *  will not exceed this number.
 *
 *  In the future, we will need to consider pagination, or bumping this
 *  limit if we find that this maximum is reached.
 */
export const DEFAULT_LIST_VIEWS_PAGE_SIZE = 1000;

/**
 * Parses a JSON string representation of a LogicalPredicate back to the object.
 * Used when fetching a view with the json field and needing to restore it for editing.
 */
export const parseJsonToLogicalPredicate = (json: string | undefined | null): LogicalPredicate | null => {
    if (!json) return null;
    try {
        return JSON.parse(json) as LogicalPredicate;
    } catch (error) {
        console.error('Failed to parse logical predicate JSON:', error);
        return null;
    }
};

/**
 * Converts a DataHubView (from the API) to ViewBuilderState for editing.
 * Parses the json field to restore the full nested logical predicate.
 */
export const convertViewToBuilderState = (view: DataHubView): ViewBuilderState => {
    const json = view.definition?.filter?.json;
    const logicalPredicate = parseJsonToLogicalPredicate(json);

    return {
        viewType: view.viewType,
        name: view.name,
        description: view.description,
        definition: {
            entityTypes: view.definition?.entityTypes,
            filter: view.definition?.filter as any,
            logicalPredicate: logicalPredicate || undefined,
            json,
        },
    };
};

/**
 * Converts an instance of the View builder state
 * into the input required to create or update a View in
 * GraphQL.
 *
 * @param state the builder state
 */
export const convertStateToUpdateInput = (state: ViewBuilderState) => {
    const logicalPredicate = state.definition?.logicalPredicate;
    const orFilters = logicalPredicate ? convertLogicalPredicateToOrFilters(logicalPredicate) : undefined;
    const json = logicalPredicate ? JSON.stringify(logicalPredicate) : undefined;

    const filterObj: any = {};

    if (orFilters !== undefined) {
        // New format: send orFilters and json for nested conditions
        filterObj.orFilters = orFilters;
        filterObj.json = json;
    } else {
        // Legacy format: send operator and filters
        filterObj.operator = state?.definition?.filter?.operator;
        filterObj.filters = state?.definition?.filter?.filters?.map((filter) => ({
            field: filter.field,
            condition: filter.condition,
            values: filter.values,
            negated: filter.negated,
        }));
    }

    return {
        viewType: state.viewType,
        name: state.name as string,
        description: state.description as string,
        definition: {
            entityTypes: state?.definition?.entityTypes,
            filter: filterObj,
        },
    };
};

/**
 * Convert ViewBuilderState and an URN into a DataHubView object.
 *
 * @param urn urn of the View
 * @param state state of the View
 */
export const convertStateToView = (urn: string, state: ViewBuilderState): DataHubView => {
    return {
        urn,
        type: EntityType.DatahubView,
        viewType: state.viewType as DataHubViewType,
        name: state.name as string,
        description: state.description,
        definition: {
            entityTypes: state.definition?.entityTypes || [],
            filter: {
                operator: state?.definition?.filter?.operator as LogicalOperator,
                filters: state?.definition?.filter?.filters?.map((filter) => ({
                    field: filter.field,
                    condition: filter.condition,
                    values: filter.values,
                    negated: filter.negated || false,
                })) as any,
            },
        },
    };
};

/**
 * Search through a list of Views by a text string by comparing
 * against View name and descriptions.
 *
 * @param views: A list of DataHub View objects.
 * @param q: An optional search query.
 */
export const searchViews = (views: Array<DataHubView>, q?: string) => {
    if (q && q.length > 0) {
        const qLower = q.toLowerCase();
        return views.filter((view) => view.name.toLowerCase().includes(qLower) || view.description?.includes(qLower));
    }
    return views;
};
