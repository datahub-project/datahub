import EntityRegistry from '@app/entityV2/EntityRegistry';
import { PropertyRow, ValueColumnData } from '@app/entityV2/shared/tabs/Properties/types';

export function parseJsonPropsToRows(jsonProps: string | null | undefined, filterText = ''): PropertyRow[] {
    if (!jsonProps) return [];
    try {
        const rawParsed = JSON.parse(jsonProps);
        if (typeof rawParsed !== 'object' || rawParsed === null || Array.isArray(rawParsed)) {
            return [];
        }
        const parsed = rawParsed as Record<string, unknown>;
        return Object.entries(parsed)
            .filter(
                ([key, value]) =>
                    !filterText ||
                    key.toLocaleLowerCase().includes(filterText.toLocaleLowerCase()) ||
                    String(value).toLocaleLowerCase().includes(filterText.toLocaleLowerCase()),
            )
            .map(([key, value]) => ({
                displayName: key,
                qualifiedName: `__jsonProp__${key}`,
                values: [{ value: String(value), entity: null }],
                type: { type: 'string', nativeDataType: 'string' },
            }));
    } catch (e) {
        console.warn('Failed to parse jsonProps for schema field:', e);
        return [];
    }
}

function matchesName(name: string, filterText: string) {
    return name.toLocaleLowerCase().includes(filterText.toLocaleLowerCase());
}

function matchesAnyFromValues(values: ValueColumnData[], filterText: string, entityRegistry: EntityRegistry) {
    return values.some(
        (value) =>
            matchesName(value.value?.toString() || '', filterText) ||
            matchesName(value.entity ? entityRegistry.getDisplayName(value.entity.type, value.entity) : '', filterText),
    );
}

export function filterStructuredProperties(
    entityRegistry: EntityRegistry,
    propertyRows: PropertyRow[],
    filterText?: string,
) {
    if (!propertyRows) return { filteredRows: [], expandedRowsFromFilter: new Set() };
    if (!filterText) return { filteredRows: propertyRows, expandedRowsFromFilter: new Set() };
    const formattedFilterText = filterText.toLocaleLowerCase();

    const finalQualifiedNames = new Set<string>();
    const expandedRowsFromFilter = new Set<string>();

    propertyRows.forEach((row) => {
        // if we match on the qualified name (maybe from a parent) do not filter out
        if (matchesName(row.qualifiedName, formattedFilterText)) {
            finalQualifiedNames.add(row.qualifiedName);
        }
        // if we match specifically on this property (not just its parent), add and expand all parents
        if (
            matchesName(row.displayName, formattedFilterText) ||
            matchesAnyFromValues(row.values || [], formattedFilterText, entityRegistry)
        ) {
            finalQualifiedNames.add(row.qualifiedName);

            const splitFieldPath = row.qualifiedName.split('.');
            splitFieldPath.reduce((previous, current) => {
                finalQualifiedNames.add(previous);
                expandedRowsFromFilter.add(previous);
                return `${previous}.${current}`;
            });
        }
    });

    const filteredRows = propertyRows.filter((row) => finalQualifiedNames.has(row.qualifiedName));

    return { filteredRows, expandedRowsFromFilter };
}
