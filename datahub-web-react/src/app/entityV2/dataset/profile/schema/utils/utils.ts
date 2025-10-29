import { SorterResult } from 'antd/lib/table/interface';
import * as diff from 'diff';

import { SchemaDiffSummary } from '@app/entityV2/dataset/profile/schema/components/SchemaVersionSummary';
import { KEY_SCHEMA_PREFIX, UNION_TOKEN, VERSION_PREFIX } from '@app/entityV2/dataset/profile/schema/utils/constants';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';
import { convertTagsForUpdate } from '@app/shared/tags/utils/convertTagsForUpdate';

import {
    EditableSchemaFieldInfo,
    EditableSchemaMetadata,
    EditableSchemaMetadataUpdate,
    PlatformSchema,
    SchemaField,
} from '@types';

export function filterKeyFieldPath(showKeySchema: boolean, field: SchemaField) {
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
    return fieldPathA === fieldPathB || downgradeV2FieldPath(fieldPathA) === downgradeV2FieldPath(fieldPathB);
}

// should use pathMatchesExact when rendering editable info so the user edits the correct field
export function pathMatchesExact(fieldPathA?: string | null, fieldPathB?: string | null) {
    return fieldPathA === fieldPathB;
}

// group schema fields by fieldPath and grouping for hierarchy in schema table
export function groupByFieldPath(
    schemaRows?: Array<SchemaField>,
    options: {
        showKeySchema: boolean;
    } = { showKeySchema: false },
): Array<ExtendedSchemaFields> {
    const rows = [
        ...(schemaRows?.filter(filterKeyFieldPath.bind({}, options.showKeySchema)) || []),
    ] as Array<ExtendedSchemaFields>;

    const outputRows: Array<ExtendedSchemaFields> = [];
    const outputRowByPath = {};

    for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
        let parentRow: null | ExtendedSchemaFields = null;
        const row = { children: undefined, ...rows[rowIndex], depth: 0 };

        for (let j = rowIndex - 1; j >= 0; j--) {
            const rowTokens = row.fieldPath.split('.');
            const isQualifyingUnionField = rowTokens[rowTokens.length - 3] === UNION_TOKEN;
            if (isQualifyingUnionField) {
                // in the case of unions, parent will not be a subset of the child
                rowTokens.splice(rowTokens.length - 2, 1);
                const parentPath = rowTokens.join('.');

                if (rows[j].fieldPath === parentPath) {
                    parentRow = outputRowByPath[rows[j].fieldPath];
                    break;
                }
            } else {
                // In the case of structs, arrays, etc, parent will be the first token from
                // the left of this field's name(last token of the path) that does not enclosed in [].
                let parentPath: null | string = null;
                for (
                    let lastParentTokenIndex = rowTokens.length - 2;
                    lastParentTokenIndex >= 0;
                    --lastParentTokenIndex
                ) {
                    const lastParentToken: string = rowTokens[lastParentTokenIndex];
                    if (lastParentToken && lastParentToken[0] !== '[') {
                        parentPath = rowTokens.slice(0, lastParentTokenIndex + 1).join('.');
                        break;
                    }
                }
                if (parentPath && rows[j].fieldPath === parentPath) {
                    parentRow = outputRowByPath[rows[j].fieldPath];
                    break;
                }
            }
        }

        // if the parent field exists in the ouput, add the current row as a child
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
