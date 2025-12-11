/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
