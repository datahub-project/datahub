import { SchemaField, GlobalTags } from '../../../types.generated';

export function urlEncodeUrn(urn: string) {
    return urn && urn.replace(/%/g, '%25').replace(/\//g, '%2F').replace(/\?/g, '%3F').replace(/#/g, '%23');
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
    let pastFields = [...(pastInputFields || [])] as Array<ExtendedSchemaFields>;
    if (!isEditMode && pastFields.length > 1) {
        // eslint-disable-next-line no-nested-ternary
        pastFields.sort((a, b) => (a.fieldPath > b.fieldPath ? 1 : b.fieldPath > a.fieldPath ? -1 : 0));
    }

    if (fields.length > 1) {
        // eslint-disable-next-line no-nested-ternary
        fields.sort((a, b) => (a.fieldPath > b.fieldPath ? 1 : b.fieldPath > a.fieldPath ? -1 : 0));
        if (!isEditMode && pastFields.length > 0) {
            fields.forEach((field, rowIndex) => {
                if (field.type === pastFields[rowIndex].type && field.fieldPath === pastFields[rowIndex].fieldPath) {
                    fields[rowIndex] = {
                        ...fields[rowIndex],
                        pastDescription: pastFields[rowIndex].description,
                        pastGlobalTags: pastFields[rowIndex].globalTags,
                    };
                    delete pastFields[rowIndex];
                } else {
                    const relevantPastFieldIndex = pastFields.findIndex(
                        (pf) => pf.type === field.type && pf.fieldPath === field.fieldPath,
                    );
                    if (relevantPastFieldIndex > -1) {
                        fields[rowIndex] = {
                            ...fields[rowIndex],
                            pastDescription: pastFields[relevantPastFieldIndex].pastDescription,
                            pastGlobalTags: pastFields[relevantPastFieldIndex].globalTags,
                        };
                        delete pastFields[relevantPastFieldIndex];
                    } else {
                        fields[rowIndex] = { ...fields[rowIndex], isNewRow: true };
                    }
                }
            });
            pastFields = pastFields.filter((pf) => !!pf);
            console.log('pastFields--', pastFields);
            if (pastFields.length > 0) {
                fields = [...fields, ...pastFields.map((pf) => ({ ...pf, isDeletedRow: true }))];
                // eslint-disable-next-line no-nested-ternary
                fields.sort((a, b) => (a.fieldPath > b.fieldPath ? 1 : b.fieldPath > a.fieldPath ? -1 : 0));
            }
        }
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
