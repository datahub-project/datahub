import { DataHubView, DataHubViewType, EntityType, LogicalOperator } from '../../../types.generated';
import { ViewBuilderState } from './types';

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
 * Converts an instance of the View builder state
 * into the input required to create or update a View in
 * GraphQL.
 *
 * @param state the builder state
 */
export const convertStateToUpdateInput = (state: ViewBuilderState) => {
    return {
        viewType: state.viewType,
        name: state.name as string,
        description: state.description as string,
        definition: {
            entityTypes: state?.definition?.entityTypes,
            filter: {
                operator: state?.definition?.filter?.operator,
                filters: state?.definition?.filter?.filters.map((filter) => ({
                    field: filter.field,
                    condition: filter.condition,
                    values: filter.values,
                    negated: filter.negated,
                })),
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
