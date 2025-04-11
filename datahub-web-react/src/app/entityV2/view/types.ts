import { DataHubViewFilter, DataHubViewType, EntityType, LogicalOperator } from '../../../types.generated';

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
    };
}
