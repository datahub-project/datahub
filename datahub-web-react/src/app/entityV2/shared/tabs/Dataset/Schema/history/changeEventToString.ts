import { downgradeV2FieldPath } from '@src/app/lineageV2/lineageUtils';
import { ChangeEvent } from '@src/types.generated';

const CATEGORY_TECHNICAL_SCHEMA = 'TECHNICAL_SCHEMA';
const CATEGORY_DOCUMENTATION = 'DOCUMENTATION';
const CATEGORY_TAG = 'TAG';
const CATEGORY_GLOSSARY_TERM = 'GLOSSARY_TERM';
const CATEGORY_OWNERSHIP = 'OWNERSHIP';
const CATEGORY_DOMAIN = 'DOMAIN';
const CATEGORY_STRUCTURED_PROPERTY = 'STRUCTURED_PROPERTY';
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

function extractNameFromUrn(urn: string): string {
    // Extract the last segment from URNs like "urn:li:tag:PII" or "urn:li:glossaryTerm:customer_id"
    const parts = urn.split(':');
    return parts[parts.length - 1] || urn;
}

function formatPropertyValues(rawJson: string): string {
    try {
        const values = JSON.parse(rawJson);
        if (Array.isArray(values)) {
            return values.map((v) => `"${v}"`).join(', ');
        }
        return `"${values}"`;
    } catch {
        return `"${rawJson}"`;
    }
}

function formatOwnerTypeUrn(urn: string): string {
    // Extract display name from URNs like "urn:li:ownershipType:__system__business_owner"
    const lastSegment = urn.split(':').pop() || '';
    // Strip the __system__ prefix if present, then humanize
    const cleaned = lastSegment.replace(/^__system__/, '');
    return cleaned
        .split('_')
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
        .join(' ');
}

function formatOwnerType(rawType: string): string {
    // Convert "TECHNICAL_OWNER" -> "Technical Owner"
    return rawType
        .split('_')
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
        .join(' ');
}

const UNINFORMATIVE_OWNER_TYPES = new Set(['NONE', 'CUSTOM']);

export function getChangeEventString(changeEvent: ChangeEvent) {
    let displayString = changeEvent.description;

    if (changeEvent.category === CATEGORY_TECHNICAL_SCHEMA) {
        const fieldPath = getParameter(changeEvent.parameters, 'fieldPath', DEFAULT_FIELD_PATH);
        if (changeEvent.operation === OPERATION_ADD) {
            displayString = `Added column ${downgradeV2FieldPath(fieldPath || '')}.`;
        } else if (changeEvent.operation === OPERATION_REMOVE) {
            displayString = `Removed column ${downgradeV2FieldPath(fieldPath || '')}.`;
        }
    } else if (changeEvent.category === CATEGORY_DOCUMENTATION) {
        const description = getParameter(changeEvent.parameters, 'description', '');

        if (!changeEvent.modifier) {
            // Documentation at the entity level
            displayString = description === '' ? EMPTY_ASSET_DOC : SET_ASSET_DOC(description);
        } else {
            // Documentation at the field level
            const fieldPath = changeEvent.modifier;
            displayString = description === '' ? EMPTY_FIELD_DOC(fieldPath) : SET_FIELD_DOC(fieldPath, description);
        }
    } else if (changeEvent.category === CATEGORY_TAG) {
        const tagUrn = getParameter(changeEvent.parameters, 'tagUrn', '');
        const tagName = tagUrn ? extractNameFromUrn(tagUrn) : 'Unknown';
        const fieldPath = getParameter(changeEvent.parameters, 'fieldPath');

        if (changeEvent.operation === OPERATION_ADD) {
            displayString = fieldPath
                ? `Added tag "${tagName}" to field ${downgradeV2FieldPath(fieldPath)}.`
                : `Added tag "${tagName}".`;
        } else if (changeEvent.operation === OPERATION_REMOVE) {
            displayString = fieldPath
                ? `Removed tag "${tagName}" from field ${downgradeV2FieldPath(fieldPath)}.`
                : `Removed tag "${tagName}".`;
        }
    } else if (changeEvent.category === CATEGORY_GLOSSARY_TERM) {
        const termUrn = getParameter(changeEvent.parameters, 'termUrn', '');
        const termName = termUrn ? extractNameFromUrn(termUrn) : 'Unknown';
        const fieldPath = getParameter(changeEvent.parameters, 'fieldPath');

        if (changeEvent.operation === OPERATION_ADD) {
            displayString = fieldPath
                ? `Added term "${termName}" to field ${downgradeV2FieldPath(fieldPath)}.`
                : `Added term "${termName}".`;
        } else if (changeEvent.operation === OPERATION_REMOVE) {
            displayString = fieldPath
                ? `Removed term "${termName}" from field ${downgradeV2FieldPath(fieldPath)}.`
                : `Removed term "${termName}".`;
        }
    } else if (changeEvent.category === CATEGORY_OWNERSHIP) {
        const ownerUrn = getParameter(changeEvent.parameters, 'ownerUrn', '');
        const ownerName = ownerUrn ? extractNameFromUrn(ownerUrn) : 'Unknown';
        const rawOwnerType = getParameter(changeEvent.parameters, 'ownerType');
        const ownerTypeUrn = getParameter(changeEvent.parameters, 'ownerTypeUrn');
        let ownerTypeSuffix = '';
        if (ownerTypeUrn) {
            ownerTypeSuffix = ` (${formatOwnerTypeUrn(ownerTypeUrn)})`;
        } else if (rawOwnerType && !UNINFORMATIVE_OWNER_TYPES.has(rawOwnerType)) {
            ownerTypeSuffix = ` (${formatOwnerType(rawOwnerType)})`;
        }

        if (changeEvent.operation === OPERATION_ADD) {
            displayString = `Added owner "${ownerName}"${ownerTypeSuffix}.`;
        } else if (changeEvent.operation === OPERATION_REMOVE) {
            displayString = `Removed owner "${ownerName}"${ownerTypeSuffix}.`;
        }
    } else if (changeEvent.category === CATEGORY_DOMAIN) {
        const domainUrn = getParameter(changeEvent.parameters, 'domainUrn', '');
        const domainName = domainUrn ? extractNameFromUrn(domainUrn) : 'Unknown';

        if (changeEvent.operation === OPERATION_ADD) {
            displayString = `Added to domain "${domainName}".`;
        } else if (changeEvent.operation === OPERATION_REMOVE) {
            displayString = `Removed from domain "${domainName}".`;
        }
    } else if (changeEvent.category === CATEGORY_STRUCTURED_PROPERTY) {
        const propertyUrn = getParameter(changeEvent.parameters, 'propertyUrn', '');
        const propertyName = propertyUrn ? extractNameFromUrn(propertyUrn) : 'Unknown';
        const propertyValues = getParameter(changeEvent.parameters, 'propertyValues');
        const valuesSuffix = propertyValues ? ` to ${formatPropertyValues(propertyValues)}` : '';

        if (changeEvent.operation === OPERATION_ADD) {
            displayString = `Set structured property "${propertyName}"${valuesSuffix}.`;
        } else if (changeEvent.operation === OPERATION_REMOVE) {
            displayString = `Removed structured property "${propertyName}".`;
        } else if (changeEvent.operation === 'MODIFY') {
            displayString = `Updated structured property "${propertyName}"${valuesSuffix}.`;
        }
    }

    return displayString;
}

/** @deprecated Use getChangeEventString instead */
export const getDocumentationString = getChangeEventString;
