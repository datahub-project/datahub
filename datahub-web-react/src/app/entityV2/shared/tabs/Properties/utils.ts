import { CustomPropertiesEntry } from '../../../../../types.generated';
import EntityRegistry from '../../../EntityRegistry';
import { GenericEntityProperties } from '../../../../entity/shared/types';
import { PropertyRow, ValueColumnData } from './types';

export function mapCustomPropertiesToPropertyRows(customProperties: CustomPropertiesEntry[]) {
    return (customProperties?.map((customProp) => ({
        displayName: customProp.key,
        values: [{ value: customProp.value || '' }],
    })) || []) as PropertyRow[];
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

export function getFilteredCustomProperties(filterText: string, entityData?: GenericEntityProperties | null) {
    return entityData?.customProperties?.filter(
        (property) => matchesName(property.key, filterText) || matchesName(property.value || '', filterText),
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
