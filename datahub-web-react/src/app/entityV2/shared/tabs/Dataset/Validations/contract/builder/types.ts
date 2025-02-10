/**
 * The Data Contract Builder state
 */
export type DataContractBuilderState = {
    /**
     * The schema contract. In the UI, we only support defining a single schema contract.
     */
    schema?: {
        assertionUrn: string;
    };

    /**
     * The freshness contract. In the UI, we only support defining a single freshness contract.
     */
    freshness?: {
        assertionUrn: string;
    };

    /**
     * Data Quality contract. We cane define multiple data quality rules as part of the contract.
     */
    dataQuality?: {
        assertionUrn: string;
    }[];
};

export const DEFAULT_BUILDER_STATE = {
    dataQuality: undefined,
    schema: undefined,
    freshness: undefined,
};

export enum DataContractCategoryType {
    FRESHNESS = 'Freshness',
    SCHEMA = 'Schema',
    DATA_QUALITY = 'Data Quality',
}
