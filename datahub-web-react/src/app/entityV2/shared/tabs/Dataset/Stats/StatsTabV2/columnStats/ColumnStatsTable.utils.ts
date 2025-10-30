import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';

// Type definitions for safe type mapping
interface SchemaField {
    fieldPath: string;
    type?: any;
    nativeDataType?: string | null;
    schemaFieldEntity?: any;
    nullable: boolean;
    recursive?: boolean;
    description?: string | null;
    children?: SchemaField[];
}

// For DatasetFieldProfile, we'll define it locally since the generated types aren't available
interface DatasetFieldProfile {
    fieldPath: string;
    nullCount?: number | null;
    nullProportion?: number | null;
    uniqueCount?: number | null;
    min?: string | null;
    max?: string | null;
}

/**
 * Infers if a field is nullable based on column statistics.
 * Uses null count or proportion data when available, defaults to nullable.
 */
export function inferIsFieldNullable(stat: DatasetFieldProfile): boolean {
    if (stat.nullCount != null) {
        return stat.nullCount > 0;
    }

    if (stat.nullProportion != null) {
        return stat.nullProportion > 0;
    }

    return true; // Default to nullable when data unavailable
}

/**
 * Safely maps unknown field data to SchemaField type.
 */
export function mapToSchemaField(field: unknown): SchemaField {
    if (!field || typeof field !== 'object') {
        throw new Error('Invalid field data provided to mapToSchemaField');
    }

    const fieldObj = field as Record<string, any>;

    return {
        fieldPath: fieldObj.fieldPath || '',
        type: fieldObj.type || null,
        nativeDataType: fieldObj.nativeDataType || null,
        schemaFieldEntity: fieldObj.schemaFieldEntity || null,
        nullable: fieldObj.nullable ?? false,
        recursive: fieldObj.recursive || false,
        description: fieldObj.description || null,
        children: fieldObj.children || undefined,
    };
}

/**
 * Safely maps an array of unknown field data to SchemaField array.
 */
export function mapToSchemaFields(fields: unknown[]): SchemaField[] {
    if (!Array.isArray(fields)) {
        return [];
    }

    return fields.map(mapToSchemaField);
}

/**
 * Creates a stats-only field object for fields that exist in column stats but not in schema.
 */
export function createStatsOnlyField(stat: DatasetFieldProfile): SchemaField {
    return {
        fieldPath: stat.fieldPath,
        type: null,
        nativeDataType: null,
        schemaFieldEntity: null,
        nullable: inferIsFieldNullable(stat),
        recursive: false,
        description: null,
    };
}

/**
 * Flattens nested field hierarchies to enable drawer field path matching.
 */
export function flattenFields(fieldList: ExtendedSchemaFields[]): ExtendedSchemaFields[] {
    const result: ExtendedSchemaFields[] = [];
    fieldList.forEach((field) => {
        result.push(field);
        if (field.children) {
            result.push(...flattenFields(field.children));
        }
    });
    return result;
}

/**
 * Handles scroll adjustment when a row is selected to ensure it's visible.
 */
export function handleRowScrollIntoView(row: HTMLTableRowElement | undefined, header: HTMLTableSectionElement | null) {
    if (!row || !header) return;

    const rowRect = row.getBoundingClientRect();
    const headerRect = header.getBoundingClientRect();
    const rowTop = rowRect.top;
    const headerBottom = headerRect.bottom;
    const scrollContainer = row.closest('table')?.parentElement;

    if (scrollContainer && rowTop < headerBottom) {
        const scrollAmount = headerBottom - rowTop;
        scrollContainer.scrollTop -= scrollAmount;
    }
}

/**
 * Filters column stats data based on search query.
 */
export function filterColumnStatsByQuery(data: any[], query: string) {
    if (!query.trim()) return data;

    const lowercaseQuery = query.toLowerCase();
    return data.filter((columnStat) => columnStat.column?.toLowerCase().includes(lowercaseQuery));
}
