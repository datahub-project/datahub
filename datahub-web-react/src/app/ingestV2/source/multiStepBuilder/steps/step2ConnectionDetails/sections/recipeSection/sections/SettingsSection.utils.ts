import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

/**
 * Settings use a compact multi-column grid only when every field is a checkbox. Mixed
 * controls (text inputs, selects) wrap badly at a third of the row, so they stack instead.
 *
 * @param fields - Every settings field for the source, including ones currently hidden.
 * @returns True when the fields should render in a single vertical column.
 */
export function shouldStackSettingsFields(fields: RecipeField[]): boolean {
    return fields.some((field) => field.type !== FieldType.BOOLEAN);
}
