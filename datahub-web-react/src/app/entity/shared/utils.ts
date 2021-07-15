import * as diff from 'diff';
import {
    EditableSchemaMetadata,
    EditableSchemaFieldInfo,
    EditableSchemaMetadataUpdate,
    SchemaField,
    GlobalTags,
} from '../../../types.generated';
import { convertTagsForUpdate } from '../../shared/tags/utils/convertTagsForUpdate';

export interface ExtendedSchemaFields extends SchemaField {
    children?: Array<ExtendedSchemaFields>;
    previousDescription?: string | null;
    pastGlobalTags?: GlobalTags | null;
    isNewRow?: boolean;
    isDeletedRow?: boolean;
}

export function urlEncodeUrn(urn: string) {
    return urn && urn.replace(/%/g, '%25').replace(/\//g, '%2F').replace(/\?/g, '%3F').replace(/#/g, '%23');
}

export function convertEditableSchemaMeta(
    editableSchemaMeta?: Array<EditableSchemaFieldInfo>,
    fields?: Array<SchemaField>,
): Array<SchemaField> {
    const updatedFields = [...(fields || [])] as Array<SchemaField>;
    if (editableSchemaMeta && editableSchemaMeta.length > 0) {
        editableSchemaMeta.forEach((updatedField) => {
            const originalFieldIndex = updatedFields.findIndex((f) => f.fieldPath === updatedField.fieldPath);
            if (originalFieldIndex > -1) {
                updatedFields[originalFieldIndex] = {
                    ...updatedFields[originalFieldIndex],
                    description: updatedField.description,
                    globalTags: { ...updatedField.globalTags },
                };
            }
        });
    }
    return updatedFields;
}

export function convertEditableSchemaMetadataForUpdate(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
): EditableSchemaMetadataUpdate {
    return {
        editableSchemaFieldInfo:
            editableSchemaMetadata?.editableSchemaFieldInfo.map((editableSchemaFieldInfo) => ({
                fieldPath: editableSchemaFieldInfo?.fieldPath,
                description: editableSchemaFieldInfo?.description,
                globalTags: { tags: convertTagsForUpdate(editableSchemaFieldInfo?.globalTags?.tags || []) },
            })) || [],
    };
}

function sortByFieldPath(row1: SchemaField, row2: SchemaField): number {
    if (row1.fieldPath > row2.fieldPath) {
        return 1;
    }
    if (row2.fieldPath > row1.fieldPath) {
        return -1;
    }
    return 0;
}

// Sort schema fields by fieldPath and grouping for hierarchy in schema table
export function sortByFieldPathAndGrouping(schemaRows?: Array<SchemaField>): Array<ExtendedSchemaFields> {
    const rows = [...(schemaRows || [])] as Array<ExtendedSchemaFields>;
    if (rows.length > 1) {
        rows.sort(sortByFieldPath); // sort rows by fieldPath value
        // repeat rows from bottom to top
        for (let rowIndex = rows.length; rowIndex--; rowIndex >= 0) {
            const row = rows[rowIndex];
            // check if fieldPath has . that indicates possibly children row or not
            if (row.fieldPath.slice(1, -1).includes('.')) {
                const fieldPaths = row.fieldPath.split(/\.(?=[^.]+$)/);
                const parentFieldIndex = rows.findIndex((f) => f.fieldPath === fieldPaths[0]);
                // add children rows into 'children' field of parent row to make the structure that antd table support
                if (parentFieldIndex > -1) {
                    if ('children' in rows[parentFieldIndex]) {
                        rows[parentFieldIndex].children?.unshift(row);
                    } else {
                        rows[parentFieldIndex] = { ...rows[parentFieldIndex], children: [row] };
                    }
                    rows.splice(rowIndex, 1);
                } else if (rowIndex > 0 && fieldPaths[0].includes(rows[rowIndex - 1].fieldPath)) {
                    if ('children' in rows[rowIndex - 1]) {
                        rows[rowIndex - 1].children?.unshift(row);
                    } else {
                        rows[rowIndex - 1] = { ...rows[rowIndex - 1], children: [row] };
                    }
                    rows.splice(rowIndex, 1);
                }
            }
        }
    }
    return rows;
}

export function diffMarkdown(oldStr: string, newStr: string) {
    const diffArray = diff.diffChars(oldStr || '', newStr || '');
    return diffArray
        .map((diffOne) => {
            if (diffOne.added) {
                return `<ins class="diff">${diffOne.value}</ins>`;
            }
            if (diffOne.removed) {
                return `<del class="diff">${diffOne.value}</del>`;
            }
            return diffOne.value;
        })
        .join('');
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

export function getRawSchema(schemaValue?: string | null): string {
    try {
        if (!schemaValue) {
            return schemaValue || '';
        }
        return JSON.stringify(JSON.parse(schemaValue), null, 2);
    } catch (e) {
        return schemaValue || '';
    }
}
