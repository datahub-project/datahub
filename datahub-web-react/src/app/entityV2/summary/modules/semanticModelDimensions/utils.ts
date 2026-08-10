import { Dataset, SchemaField, SemanticFieldType } from '@types';

export type DimensionPillKind = 'time' | 'calculated' | 'plain';

export type DimensionGroup = {
    dataset: Dataset;
    fields: SchemaField[];
};

export function isDefaultAliasQualifiedField(expression: string, fieldPath: string): boolean {
    const normalized = expression.toLowerCase();
    const path = fieldPath.toLowerCase();
    return normalized === path || normalized.endsWith(`.${path}`);
}

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

export function getDimensionPillKind(field: SchemaField): DimensionPillKind {
    if (field.schemaFieldEntity?.semanticFieldAnnotation?.dimension?.isTime) {
        return 'time';
    }
    if (isCalculatedDimension(field)) {
        return 'calculated';
    }
    return 'plain';
}

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
