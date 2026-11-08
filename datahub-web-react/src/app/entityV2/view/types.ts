import { LogicalOperatorType, LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';

import { DataHubViewFilter, DataHubViewType, EntityType, LogicalOperator } from '@types';

/**
 * Default builder state when creating a new View.
 */
export const DEFAULT_BUILDER_STATE = {
    viewType: DataHubViewType.Personal,
    name: '',
    description: null,
    definition: {
        entityTypes: [],
        filter: {
            operator: LogicalOperator.And,
            filters: [],
        },
        logicalPredicate: {
            type: 'logical' as const,
            operator: LogicalOperatorType.AND,
            operands: [{ type: 'property' as const }],
        },
    },
} as ViewBuilderState;

/**
 * The object represents the state of the Test Builder form.
 */
export interface ViewBuilderState {
    /**
     * The type of the View
     */
    viewType?: DataHubViewType;

    /**
     * The name of the View.
     */
    name?: string;

    /**
     * An optional description for the View.
     */
    description?: string | null;

    /**
     * The definition of the View
     */
    definition?: {
        /**
         * The Entity Types in scope for the View.
         */
        entityTypes?: EntityType[] | null;

        /**
         * The Filter for the View.
         */
        filter?: DataHubViewFilter;

        /**
         * The nested logical predicate for the Build Filters tab.
         * This stores the full nested AND/OR/NOT structure for editing.
         */
        logicalPredicate?: LogicalPredicate | null;

        /**
         * JSON string representation of the logical predicate for API storage.
         * Used for preserving arbitrary nesting when saving/fetching views.
         */
        json?: string | null;
    };
}
