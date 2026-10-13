import { Dataset, SchemaField, SemanticFieldType } from '@types';

export type DimensionPillKind = 'time' | 'calculated' | 'plain';

export type DimensionGroup = {
    dataset: Dataset;
    fields: SchemaField[];
};

/** True when the SQL expression is only a bare field path or a single alias-qualified reference. */
export function isDefaultAliasQualifiedField(expression: string, fieldPath: string): boolean {
    const normalized = expression.toLowerCase().trim();
    const path = fieldPath.toLowerCase();
    if (normalized === path) {
        return true;
    }

    const qualifiedSuffix = `.${path}`;
    if (!normalized.endsWith(qualifiedSuffix)) {
        return false;
    }

    const alias = normalized.slice(0, -qualifiedSuffix.length);
    return /^[a-z_][a-z0-9_]*$/.test(alias);
}

/** True when any dialect expression differs from the default alias-qualified field reference. */
export function isCalculatedDimension(field: SchemaField): boolean {
    const fieldPath = field.fieldPath ?? '';
    const annotation = field.schemaFieldEntity?.semanticFieldAnnotation;
    return (annotation?.expression?.dialects ?? []).some((dialect) => {
        const expression = (dialect.expression ?? '').trim();
        if (!expression) {
            return false;
        }
        return !isDefaultAliasQualifiedField(expression, fieldPath);
    });
}

/** Maps a dimension field to the pill variant shown in the summary module. */
export function getDimensionPillKind(field: SchemaField): DimensionPillKind {
    if (field.schemaFieldEntity?.semanticFieldAnnotation?.dimension?.isTime) {
        return 'time';
    }
    if (isCalculatedDimension(field)) {
        return 'calculated';
    }
    return 'plain';
}

/** Groups member datasets that contain at least one semantic dimension field. */
export function getDimensionGroups(datasets: Dataset[]): DimensionGroup[] {
    return datasets
        .map((dataset): DimensionGroup | null => {
            const fields = (dataset.schema?.fields ?? []).filter(
                (field) => field.schemaFieldEntity?.semanticFieldAnnotation?.type === SemanticFieldType.Dimension,
            );
            if (!fields.length) {
                return null;
            }
            return { dataset, fields };
        })
        .filter((group): group is DimensionGroup => group !== null);
}
