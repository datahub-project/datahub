import { SorterResult } from 'antd/lib/table/interface';
import * as diff from 'diff';

import { KEY_SCHEMA_PREFIX, UNION_TOKEN, VERSION_PREFIX } from '@app/entityV2/dataset/profile/schema/utils/constants';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';

import { PlatformSchema, SchemaField } from '@types';

export function filterKeyFieldPath(showKeySchema: boolean | undefined, field: SchemaField) {
    if (showKeySchema === undefined) return true;
    return field.fieldPath.indexOf(KEY_SCHEMA_PREFIX) > -1 ? showKeySchema : !showKeySchema;
}

export function downgradeV2FieldPath(fieldPath?: string | null) {
    if (!fieldPath) {
        return fieldPath;
    }

    const cleanedFieldPath = fieldPath.replace(KEY_SCHEMA_PREFIX, '').replace(VERSION_PREFIX, '');

    // Remove all bracket annotations (e.g., [0], [*], [key]) from the field path
    return cleanedFieldPath
        .split('.')
        .map((segment) => {
            // Remove segments that are entirely brackets (e.g., "[0]", "[*]")
            if (segment.startsWith('[') && segment.endsWith(']')) {
                return null;
            }
            // Remove bracket suffixes from segments (e.g., "addresses[0]" -> "addresses")
            return segment.replace(/\[[^\]]*\]/g, '');
        })
        .filter(Boolean)
        .join('.');
}

export function pathMatchesNewPath(fieldPathA?: string | null, fieldPathB?: string | null) {
    return fieldPathA === fieldPathB || fieldPathA === downgradeV2FieldPath(fieldPathB);
}

export function pathMatchesInsensitiveToV2(fieldPathA?: string | null, fieldPathB?: string | null) {
    if (!fieldPathA || !fieldPathB) return false;
    if (fieldPathA === fieldPathB) return true;
    const a = downgradeV2FieldPath(fieldPathA);
    const b = downgradeV2FieldPath(fieldPathB);
    return !!a && !!b && a.toLowerCase() === b.toLowerCase();
}

// should use pathMatchesExact when rendering editable info so the user edits the correct field
export function pathMatchesExact(fieldPathA?: string | null, fieldPathB?: string | null) {
    return fieldPathA === fieldPathB;
}

// Compute the expected parent fieldPath for a given fieldPath in O(path-depth) time,
// without scanning previously-seen sibling rows. Returns null for top-level fields.
export function getParentPath(fieldPath: string): string | null {
    const tokens = fieldPath.split('.');
    const isQualifyingUnionField = tokens[tokens.length - 3] === UNION_TOKEN;

    if (isQualifyingUnionField) {
        // For unions the parent path drops the union variant label (penultimate token).
        const parentTokens = [...tokens];
        parentTokens.splice(parentTokens.length - 2, 1);
        return parentTokens.join('.');
    }

    // For structs/arrays find the rightmost non-bracket token to the left of the leaf.
    for (let i = tokens.length - 2; i >= 0; i--) {
        if (tokens[i] && tokens[i][0] !== '[') {
            return tokens.slice(0, i + 1).join('.');
        }
    }
    return null;
}

// group schema fields by fieldPath and grouping for hierarchy in schema table
export function groupByFieldPath(
    schemaRows?: Array<SchemaField>,
    options: {
        showKeySchema: boolean | undefined;
    } = { showKeySchema: false },
): Array<ExtendedSchemaFields> {
    const rows = [
        ...(schemaRows?.filter(filterKeyFieldPath.bind({}, options.showKeySchema)) || []),
    ] as Array<ExtendedSchemaFields>;

    const outputRows: Array<ExtendedSchemaFields> = [];
    // keyed by fieldPath so parent lookup is O(1) — replaces the O(n) inner loop
    const outputRowByPath: Record<string, ExtendedSchemaFields> = {};

    for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
        const row = { children: undefined, ...rows[rowIndex], depth: 0 };

        const parentPath = getParentPath(row.fieldPath);
        const parentRow = parentPath ? (outputRowByPath[parentPath] ?? null) : null;

        // if the parent field exists in the output, add the current row as a child
        if (parentRow) {
            row.depth = (parentRow.depth || 0) + 1;
            row.parent = parentRow;
            parentRow.children = [...(parentRow.children || []), row];
        } else {
            outputRows.push(row);
        }
        outputRowByPath[row.fieldPath] = row;
    }
    return outputRows;
}

export function diffJson(oldStr: string, newStr: string) {
    const diffArray = diff.diffJson(oldStr || '', newStr || '');
    return diffArray
        .map((diffOne) => {
            if (diffOne.added) {
                return `+${diffOne.value}`;
            }
            if (diffOne.removed) {
                return `-${diffOne.value}`;
            }
            return diffOne.value;
        })
        .join('');
}

export function formatRawSchema(schemaValue?: string | null): string {
    try {
        if (!schemaValue) {
            return schemaValue || '';
        }
        return JSON.stringify(JSON.parse(schemaValue), null, 2);
    } catch (e) {
        return schemaValue || '';
    }
}

export function getRawSchema(schema: PlatformSchema | undefined | null, showKeySchema: boolean): string {
    if (!schema) {
        return '';
    }

    if (schema.__typename === 'TableSchema') {
        return schema.schema;
    }
    if (schema.__typename === 'KeyValueSchema') {
        return showKeySchema ? schema.keySchema : schema.valueSchema;
    }
    return '';
}

// we need to calculate excluding collapsed fields because Antd table expects
// an indexToScroll to only counting based on visible fields
export function findIndexOfFieldPathExcludingCollapsedFields(
    fieldPath: string,
    expandedRows: Set<string>,
    rows: Array<ExtendedSchemaFields>,
    sorter: SorterResult<any> | undefined,
    compareFn: ((a: any, b: any) => number) | undefined,
) {
    let index = 0; // This will keep track of the index across recursive calls

    function search(shadowedRows) {
        let sortedRows = shadowedRows;
        if (sorter?.order === 'ascend') {
            sortedRows = shadowedRows.toSorted(compareFn);
        } else if (sorter?.order === 'descend') {
            sortedRows = shadowedRows.toSorted(compareFn).toReversed();
        }

        // eslint-disable-next-line no-restricted-syntax
        for (const row of sortedRows) {
            // eslint-disable-next) {
            // Check if the current row's ID matches the ID we're looking for
            if (row.fieldPath === fieldPath) {
                return index;
            }
            index++; // Increment index for the current row

            // Check if current row is expanded and has children
            if (expandedRows.has(row.fieldPath) && row.children && row.children.length) {
                const foundIndex = search(row.children); // Recursively search children
                if (foundIndex !== -1) {
                    // If found in children, return the found index
                    return foundIndex;
                }
            }
        }
        // Return -1 if the ID was not found in this branch
        return -1;
    }

    // Start the recursive search
    return search(rows);
}
