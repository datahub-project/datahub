import { FacetFilter } from '@types';

export enum ViewBuilderMode {
    /**
     * See a View definition in Preview Mode.
     */
    PREVIEW,
    /**
     * Create or Edit a View.
     */
    EDITOR,
}

/**
 * Represents a single filter criterion within a View definition.
 * Used as the intermediate representation between the UI filter tabs
 * and the backend-compatible FacetFilter format.
 */
export type ViewFilter = {
    field: string;
    values: string[];
    condition?: FacetFilter['condition'];
    negated?: boolean;
};
