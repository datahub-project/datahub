import * as diff from 'diff';
import { EditableSchemaFieldInfo, SchemaField, GlobalTags } from '../../../types.generated';

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

export interface ExtendedSchemaFields extends SchemaField {
    children?: Array<ExtendedSchemaFields>;
    pastDescription?: string | null;
    pastGlobalTags?: GlobalTags | null;
    isNewRow?: boolean;
    isDeletedRow?: boolean;
}

export function fieldPathSortAndParse(
    inputFields?: Array<SchemaField>,
    pastInputFields?: Array<SchemaField>,
    isEditMode = true,
): Array<ExtendedSchemaFields> {
    let fields = [...(inputFields || [])] as Array<ExtendedSchemaFields>;
    const pastFields = [...(pastInputFields || [])] as Array<ExtendedSchemaFields>;

    if (!isEditMode && pastFields.length > 0) {
        fields.forEach((field, rowIndex) => {
            const relevantPastFieldIndex = pastFields.findIndex(
                (pf) => pf.type === fields[rowIndex].type && pf.fieldPath === fields[rowIndex].fieldPath,
            );
            if (relevantPastFieldIndex > -1) {
                fields[rowIndex] = {
                    ...fields[rowIndex],
                    pastDescription: pastFields[relevantPastFieldIndex].description,
                    pastGlobalTags: pastFields[relevantPastFieldIndex].globalTags,
                };
                pastFields.splice(relevantPastFieldIndex, 1);
            } else {
                fields[rowIndex] = { ...fields[rowIndex], isNewRow: true };
            }
        });
        if (pastFields.length > 0) {
            fields = [...fields, ...pastFields.map((pf) => ({ ...pf, isDeletedRow: true }))];
        }
    }

    if (fields.length > 1) {
        // eslint-disable-next-line no-nested-ternary
        fields.sort((a, b) => (a.fieldPath > b.fieldPath ? 1 : b.fieldPath > a.fieldPath ? -1 : 0));
        for (let rowIndex = fields.length; rowIndex--; rowIndex >= 0) {
            const field = fields[rowIndex];
            if (field.fieldPath.slice(1, -1).includes('.')) {
                const fieldPaths = field.fieldPath.split(/\.(?=[^.]+$)/);
                const parentFieldIndex = fields.findIndex((f) => f.fieldPath === fieldPaths[0]);
                if (parentFieldIndex > -1) {
                    if ('children' in fields[parentFieldIndex]) {
                        fields[parentFieldIndex].children?.unshift(field);
                    } else {
                        fields[parentFieldIndex] = { ...fields[parentFieldIndex], children: [field] };
                    }
                    fields.splice(rowIndex, 1);
                } else if (rowIndex > 0 && fieldPaths[0].includes(fields[rowIndex - 1].fieldPath)) {
                    if ('children' in fields[rowIndex - 1]) {
                        fields[rowIndex - 1].children?.unshift(field);
                    } else {
                        fields[rowIndex - 1] = { ...fields[rowIndex - 1], children: [field] };
                    }
                    fields.splice(rowIndex, 1);
                }
            }
        }
    }

    return fields;
}

export function diffMarkdown(oldStr: string, newStr: string) {
    const diffArray = diff.diffChars(oldStr || '', newStr || '');
    return diffArray
        .map((diffOne) =>
            // eslint-disable-next-line no-nested-ternary
            diffOne.added
                ? `<ins class="diff">${diffOne.value}</ins>`
                : diffOne.removed
                ? `<del class="diff">${diffOne.value}</del>`
                : diffOne.value,
        )
        .join('');
}
