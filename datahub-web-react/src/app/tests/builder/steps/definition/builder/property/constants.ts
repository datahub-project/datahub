/**
 * This is a placeholder ID that is used in the Property Select experience to switch into the "Structured Property"
 * builder experience.
 *
 * Once this property value is selected, we will automatically display the Structured Property predicate builder, which
 * allows you to select a specific structured property as the target for metadata test evaluation.
 */
export const STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID = '__structuredPropertyRef';

/**
 * Regex used to match a property that references a specific structured property
 */
export const STRUCTURED_PROPERTY_REFERENCE_REGEX = /^structuredProperties\.(urn:li:structuredProperty:.+)$/;
