import { downgradeV2FieldPath } from '@src/app/lineageV2/lineageUtils';
import { ChangeEvent } from '@src/types.generated';

const CATEGORY_TECHNICAL_SCHEMA = 'TECHNICAL_SCHEMA';
const CATEGORY_DOCUMENTATION = 'DOCUMENTATION';
const OPERATION_ADD = 'ADD';
const OPERATION_REMOVE = 'REMOVE';
const DEFAULT_FIELD_PATH = 'A new field';
const EMPTY_ASSET_DOC = 'Asset documentation is empty.';
const EMPTY_FIELD_DOC = (fieldPath) => `Field documentation for ${downgradeV2FieldPath(fieldPath)} is empty.`;
const SET_ASSET_DOC = (description) => `Set asset documentation to ${description}`;
const SET_FIELD_DOC = (fieldPath, description) =>
    `Set field documentation for ${downgradeV2FieldPath(fieldPath)} to ${description}`;

// function that iterates through array of key, value objects and returns the value associated with the key or default value
// if the key is not found
function getParameter(
    parameters?: Array<{ key?: string | undefined | null; value?: string | undefined | null }> | null,
    key?: string,
    defaultValue?: string,
): string | undefined {
    const parameter = (parameters || []).find((param) => param.key === key);
    return parameter?.value || defaultValue;
}

export function getDocumentationString(changeEvent: ChangeEvent) {
    let documentationString = changeEvent.description;

    if (changeEvent.category === CATEGORY_TECHNICAL_SCHEMA) {
        const fieldPath = getParameter(changeEvent.parameters, 'fieldPath', DEFAULT_FIELD_PATH);
        if (changeEvent.operation === OPERATION_ADD) {
            documentationString = `Added column ${downgradeV2FieldPath(fieldPath || '')}.`;
        } else if (changeEvent.operation === OPERATION_REMOVE) {
            documentationString = `Removed column ${downgradeV2FieldPath(fieldPath || '')}.`;
        }
    } else if (changeEvent.category === CATEGORY_DOCUMENTATION) {
        const description = getParameter(changeEvent.parameters, 'description', '');

        if (!changeEvent.modifier) {
            // Documentation at the entity level
            documentationString = description === '' ? EMPTY_ASSET_DOC : SET_ASSET_DOC(description);
        } else {
            // Documentation at the field level
            const fieldPath = changeEvent.modifier;
            documentationString =
                description === '' ? EMPTY_FIELD_DOC(fieldPath) : SET_FIELD_DOC(fieldPath, description);
        }
    }

    return documentationString;
}
