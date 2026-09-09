export type DataProductBuilderState = {
    name: string;
    description?: string;
    /** URN of the optional parent Data Product. */
    parentDataProductUrn?: string;
    /** Display name for the selected parent (UI only). */
    parentDataProductName?: string;
};
