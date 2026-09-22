import { filtersToLogicalPredicate } from '@app/entityV2/view/builder/utils';
import { ViewBuilderState } from '@app/entityV2/view/types';
import { LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { convertLogicalPredicateToOrFilters } from '@app/sharedV2/queryBuilder/builder/utils';

import { DataHubView, DataHubViewType, EntityType, LogicalOperator } from '@types';

/**
 * Recursively cleans a LogicalPredicate tree:
 * - Removes PropertyPredicates without a property (empty conditions)
 * - Defaults operator to "equals" if property exists but operator is missing
 * - Removes LogicalPredicates that end up with no valid operands
 */
export const cleanLogicalPredicate = (
    predicate: LogicalPredicate | PropertyPredicate | undefined | null,
): LogicalPredicate | PropertyPredicate | undefined => {
    if (!predicate) return undefined;

    if (predicate.type === 'property') {
        const prop = predicate as PropertyPredicate;
        // Remove if no property
        if (!prop.property) return undefined;
        // If property exists but operator missing, default to "equals"
        if (!prop.operator) {
            return { ...prop, operator: 'equals' };
        }
        return predicate;
    }

    const logical = predicate as LogicalPredicate;
    const cleanedOperands = (logical.operands ?? [])
        .map((op) => cleanLogicalPredicate(op))
        .filter((op) => op !== undefined) as (LogicalPredicate | PropertyPredicate)[];

    if (cleanedOperands.length === 0) {
        return undefined;
    }

    return {
        ...logical,
        operands: cleanedOperands,
    };
};

/**
 * Recursively checks if a LogicalPredicate has at least one valid condition.
 * A valid PropertyPredicate must have a property field selected.
 * (Operator defaults to "equals" during save if not selected, values are optional).
 */
export const hasAtLeastOneValidCondition = (
    predicate: LogicalPredicate | PropertyPredicate | undefined | null,
): boolean => {
    if (!predicate) return false;
    if (predicate.type === 'property') {
        return !!(predicate as PropertyPredicate).property;
    }
    const logical = predicate as LogicalPredicate;
    return (logical.operands ?? []).some(hasAtLeastOneValidCondition);
};

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
 * Ensures logicalPredicate is always populated:
 * - If json field exists, parse it to restore the full nested structure
 * - If no json but legacy filters exist, convert them to logicalPredicate
 * This guarantees all views use a consistent internal representation.
 */
export const convertViewToBuilderState = (view: DataHubView): ViewBuilderState => {
    const json = view.definition?.filter?.json;
    const filter = view.definition?.filter;

    let logicalPredicate = parseJsonToLogicalPredicate(json);

    if (!logicalPredicate && filter?.filters) {
        const viewFilters = filter.filters.map((f) => ({
            field: f.field,
            condition: f.condition,
            values: f.values,
            negated: f.negated || undefined,
        }));
        logicalPredicate = filtersToLogicalPredicate(filter.operator, viewFilters);
    }

    return {
        viewType: view.viewType,
        name: view.name,
        description: view.description,
        definition: {
            entityTypes: view.definition?.entityTypes,
            filter: filter as any,
            logicalPredicate: logicalPredicate || undefined,
            json,
        },
    };
};

/**
 * Converts ViewBuilderState into the GraphQL input for creating/updating a View.
 * Always sends the new format (orFilters + json); legacy format (operator + filters) is deprecated.
 * Assumes logicalPredicate is always populated (convertViewToBuilderState guarantees this).
 */
export const convertStateToUpdateInput = (state: ViewBuilderState) => {
    const logicalPredicate = state.definition?.logicalPredicate;

    if (!logicalPredicate) {
        throw new Error('logicalPredicate must be populated. convertViewToBuilderState should ensure this.');
    }

    // Clean empty conditions before serializing to API
    const cleanedPredicate = cleanLogicalPredicate(logicalPredicate);

    if (!cleanedPredicate || cleanedPredicate.type !== 'logical' || !cleanedPredicate.operands?.length) {
        throw new Error('View must have at least one valid filter condition');
    }

    const orFilters = convertLogicalPredicateToOrFilters(cleanedPredicate);
    const json = JSON.stringify(cleanedPredicate);

    return {
        viewType: state.viewType,
        name: state.name as string,
        description: state.description as string,
        definition: {
            entityTypes: state?.definition?.entityTypes,
            filter: {
                orFilters,
                json,
            },
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
