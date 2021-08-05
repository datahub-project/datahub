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

export function notEmpty<TValue>(value: TValue | null | undefined): value is TValue {
    return value !== null && value !== undefined;
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

// Sort schema fields by fieldPath and grouping for hierarchy in schema table
export function groupByFieldPath(schemaRows?: Array<SchemaField>): Array<ExtendedSchemaFields> {
    const rows = [...(schemaRows || [])] as Array<ExtendedSchemaFields>;
    const outputRows: Array<ExtendedSchemaFields> = [];
    const outputRowByPath = {};

    for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
        const row = { children: undefined, ...rows[rowIndex] };

        const [parentFieldPath] = row.fieldPath.split(/\.(?=[^.]+$)/);
        const parentRow = outputRowByPath[parentFieldPath];

        // if the parent field exists in the ouput, add the current row as a child
        if (parentRow) {
            parentRow.children = [...(parentRow.children || []), row];
        } else {
            outputRows.push(row);
        }
        outputRowByPath[row.fieldPath] = row;
    }
    return outputRows;
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
