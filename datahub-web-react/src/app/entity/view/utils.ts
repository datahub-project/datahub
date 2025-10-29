import { ViewBuilderState } from '@app/entity/view/types';

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
                filters: state?.definition?.filter?.filters?.map((filter) => ({
                    field: filter.field,
                    condition: filter.condition,
                    values: filter.values,
                    negated: filter.negated,
                })),
            },
        },
    };
};
