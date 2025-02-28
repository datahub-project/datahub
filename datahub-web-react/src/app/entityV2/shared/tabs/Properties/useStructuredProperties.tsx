import { PropertyValue, StructuredPropertiesEntry } from '../../../../../types.generated';
import { useEntityData } from '../../../../entity/shared/EntityContext';
import { GenericEntityProperties } from '../../../../entity/shared/types';
import { getStructuredPropertyValue } from '../../../../entity/shared/utils';
import EntityRegistry from '../../../EntityRegistry';
import { useGetEntityWithSchema } from '../Dataset/Schema/useGetEntitySchema';
import { PropertyRow } from './types';
import { filterStructuredProperties } from './utils';

const typeNameToType = {
    StringValue: { type: 'string', nativeDataType: 'text' },
    NumberValue: { type: 'number', nativeDataType: 'float' },
};

export function mapStructuredPropertyValues(structuredPropertiesEntry: StructuredPropertiesEntry) {
    return structuredPropertiesEntry.values
        .filter((value) => !!value)
        .map((value) => ({
            value: getStructuredPropertyValue(value as PropertyValue),
            entity:
                structuredPropertiesEntry.valueEntities?.find(
                    (entity) => entity?.urn === getStructuredPropertyValue(value as PropertyValue),
                ) || null,
        }));
}

export function mapStructuredPropertyToPropertyRow(structuredPropertiesEntry: StructuredPropertiesEntry) {
    const { displayName, qualifiedName } = structuredPropertiesEntry.structuredProperty.definition;
    return {
        displayName: displayName || qualifiedName,
        qualifiedName,
        values: mapStructuredPropertyValues(structuredPropertiesEntry),
        dataType: structuredPropertiesEntry.structuredProperty.definition.valueType,
        structuredProperty: structuredPropertiesEntry.structuredProperty,
        type:
            structuredPropertiesEntry.values[0] && structuredPropertiesEntry.values[0].__typename
                ? {
                      type: typeNameToType[structuredPropertiesEntry.values[0].__typename].type,
                      nativeDataType: typeNameToType[structuredPropertiesEntry.values[0].__typename].nativeDataType,
                  }
                : undefined,
        associatedUrn: structuredPropertiesEntry.associatedUrn,
    };
}

// map the properties map into a list of PropertyRow objects to render in a table
function getStructuredPropertyRows(entityData?: GenericEntityProperties | null) {
    const structuredPropertyRows: PropertyRow[] = [];

    entityData?.structuredProperties?.properties
        ?.filter((prop) => prop.structuredProperty.exists)
        .forEach((structuredPropertiesEntry) => {
            structuredPropertyRows.push(mapStructuredPropertyToPropertyRow(structuredPropertiesEntry));
        });

    return structuredPropertyRows;
}

function getFieldStructuredPropertyRows(fieldPath: string, entityData?: GenericEntityProperties | null) {
    const structuredPropertyRows: PropertyRow[] = [];

    const schemaFieldEntity = entityData?.schemaMetadata?.fields?.find(
        (f) => f.fieldPath === fieldPath,
    )?.schemaFieldEntity;

    schemaFieldEntity?.structuredProperties?.properties
        ?.filter((prop) => prop.structuredProperty.exists)
        .forEach((structuredPropertiesEntry) => {
            structuredPropertyRows.push(mapStructuredPropertyToPropertyRow(structuredPropertiesEntry));
        });

    return structuredPropertyRows;
}

export function findAllSubstrings(s: string): Array<string> {
    const substrings: Array<string> = [];

    for (let i = 0; i < s.length; i++) {
        if (s[i] === '.') {
            substrings.push(s.substring(0, i));
        }
    }
    substrings.push(s);
    return substrings;
}

export function createParentPropertyRow(displayName: string, qualifiedName: string): PropertyRow {
    return {
        displayName,
        qualifiedName,
        isParentRow: true,
    };
}

export function identifyAndAddParentRows(rows?: Array<PropertyRow>): Array<PropertyRow> {
    /**
     * This function takes in an array of PropertyRow objects and determines which rows are parents. These parents need
     * to be extracted in order to organize the rows into a properly nested structure later on. The final product returned
     * is a list of parent rows, without values or children assigned.
     */
    const qualifiedNames: Array<string> = [];

    // Get list of fqns
    if (rows) {
        rows.forEach((row) => {
            qualifiedNames.push(row.qualifiedName);
        });
    }

    const finalParents: PropertyRow[] = [];
    const finalParentNames = new Set();

    // Loop through list of fqns and find all substrings.
    // e.g. a.b.c.d becomes a, a.b, a.b.c, a.b.c.d
    qualifiedNames.forEach((fqn) => {
        let previousCount: number | null = null;
        let previousParentName = '';

        const substrings = findAllSubstrings(fqn);

        // Loop through substrings and count how many other fqns have that substring in them. Use this to determine
        // if a property should be nested. If the count is equal then we should not nest, because there's no split
        // that would tell us to nest. If the count is not equal, we should nest the child properties.
        for (let index = 0; index < substrings.length; index++) {
            const token = substrings[index];
            const currentCount = qualifiedNames.filter((name) => name.startsWith(`${token}.`)).length;

            // If there's only one child, don't nest it
            if (currentCount === 1) {
                break;
            }

            // Add previous fqn, or,previousParentName, if we have found a viable parent path
            if (previousCount !== null && previousCount !== currentCount) {
                if (!finalParentNames.has(previousParentName)) {
                    const parent: PropertyRow = createParentPropertyRow(previousParentName, previousParentName);
                    parent.childrenCount = previousCount;
                    finalParentNames.add(previousParentName);
                    finalParents.push(parent);
                }
            }

            previousCount = currentCount;
            previousParentName = token;
        }
    });

    return finalParents;
}

export function groupByParentProperty(rows?: Array<PropertyRow>): Array<PropertyRow> {
    /**
     * This function takes in an array of PropertyRow objects, representing parent and child properties. Parent properties
     * will not have values, but child properties will. It organizes the rows into the parent and child structure and
     * returns a list of PropertyRow objects representing it.
     */
    const outputRows: Array<PropertyRow> = [];
    const outputRowByPath = {};

    if (rows) {
        // Iterate through all rows
        for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
            let parentRow: null | PropertyRow = null;
            const row = { children: undefined, ...rows[rowIndex], depth: 0 };

            // Iterate through a row's characters, and split the row's path into tokens
            // e.g. a, b, c for the example a.b.c
            for (let j = rowIndex - 1; j >= 0; j--) {
                const rowTokens = row.qualifiedName.split('.');
                let parentPath: null | string = null;
                let previousParentPath = rowTokens.slice(0, rowTokens.length - 1).join('.');

                // Iterate through a row's path backwards, and check if the previous row's path has been seen. If it has,
                // populate parentRow. If not, move on to the next path token.
                // e.g. for a.b.c.d, first evaluate a.b.c to see if it has been seen. If it hasn't, move to a.b
                for (
                    let lastParentTokenIndex = rowTokens.length - 2;
                    lastParentTokenIndex >= 0;
                    --lastParentTokenIndex
                ) {
                    const lastParentToken: string = rowTokens[lastParentTokenIndex];
                    if (lastParentToken && Object.keys(outputRowByPath).includes(previousParentPath)) {
                        parentPath = rowTokens.slice(0, lastParentTokenIndex + 1).join('.');
                        break;
                    }
                    previousParentPath = rowTokens.slice(0, lastParentTokenIndex).join('.');
                }

                if (parentPath && rows[j].qualifiedName === parentPath) {
                    parentRow = outputRowByPath[rows[j].qualifiedName];
                    break;
                }
            }

            // If the parent row exists in the ouput, add the current row as a child. If not, add the current row
            // to the final output.
            if (parentRow) {
                row.depth = (parentRow.depth || 0) + 1;
                row.parent = parentRow;
                if (row.isParentRow) {
                    row.displayName = row.displayName.replace(`${parentRow.displayName}.`, '');
                }
                parentRow.children = [...(parentRow.children || []), row];
            } else {
                outputRows.push(row);
            }
            outputRowByPath[row.qualifiedName] = row;
        }
    }
    return outputRows;
}

export default function useStructuredProperties(
    entityRegistry: EntityRegistry,
    fieldPath: string | null,
    filterText?: string,
) {
    const { entityData } = useEntityData();
    const { entityWithSchema } = useGetEntityWithSchema(!fieldPath);

    let structuredPropertyRowsRaw: PropertyRow[] = [];
    if (fieldPath) {
        structuredPropertyRowsRaw = getFieldStructuredPropertyRows(
            fieldPath,
            entityWithSchema as GenericEntityProperties,
        );
    } else {
        structuredPropertyRowsRaw = getStructuredPropertyRows(entityData);
    }
    const parentRows = identifyAndAddParentRows(structuredPropertyRowsRaw);

    structuredPropertyRowsRaw = [...structuredPropertyRowsRaw, ...parentRows];

    const { filteredRows, expandedRowsFromFilter } = filterStructuredProperties(
        entityRegistry,
        structuredPropertyRowsRaw,
        filterText,
    );

    // Sort by fqn before nesting algorithm
    const copy = [...filteredRows].sort((a, b) => {
        return a.qualifiedName.localeCompare(b.qualifiedName);
    });

    // group properties by path
    const structuredPropertyRows = groupByParentProperty(copy);

    return {
        structuredPropertyRows,
        expandedRowsFromFilter: expandedRowsFromFilter as Set<string>,
        structuredPropertyRowsRaw,
    };
}
