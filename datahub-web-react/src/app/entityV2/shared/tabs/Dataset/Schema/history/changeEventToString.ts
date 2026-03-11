import {
    CATEGORY_APPLICATION,
    CATEGORY_DOMAIN,
    CATEGORY_STRUCTURED_PROPERTY,
    ChangeCategoryType,
    ChangeOperationType,
    GLOSSARY_RELATIONSHIP_TYPE_LABELS,
    PARAM_DESCRIPTION,
    PARAM_DOMAIN_URN,
    PARAM_FIELD_PATH,
    PARAM_OWNER_TYPE,
    PARAM_OWNER_TYPE_URN,
    PARAM_OWNER_URN,
    PARAM_PROPERTY_URN,
    PARAM_PROPERTY_VALUES,
    PARAM_RELATIONSHIP_TYPE,
    PARAM_TAG_URN,
    PARAM_TERM_URN,
    UNINFORMATIVE_OWNER_TYPES,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar.utils';
import { downgradeV2FieldPath } from '@src/app/lineageV2/lineageUtils';
import { ChangeEvent } from '@src/types.generated';

const DEFAULT_FIELD_PATH = 'A new field';
const EMPTY_ASSET_DOC = 'Asset documentation is empty.';
const EMPTY_FIELD_DOC = (fieldPath) => `Field documentation for ${downgradeV2FieldPath(fieldPath)} is empty.`;
const SET_ASSET_DOC = (description) => `Set asset documentation to ${description}`;
const SET_FIELD_DOC = (fieldPath, description) =>
    `Set field documentation for ${downgradeV2FieldPath(fieldPath)} to ${description}`;

function getRelationshipLabel(relationshipType: string): string {
    return GLOSSARY_RELATIONSHIP_TYPE_LABELS[relationshipType] || 'related';
}

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

/**
 * Strip entity URN references from backend descriptions since they are redundant
 * in the Change History sidebar (we're already viewing that entity's history).
 * Handles patterns like: "for 'urn:li:...'", "of 'urn:li:...'", "to entity 'urn:li:...'"
 */
export function stripEntityUrns(text: string | undefined | null): string {
    if (!text) return '';
    return text
        .replace(/ (?:to|from) entity 'urn:li:[^']*'/g, '')
        .replace(/ (?:for|of|to|from) 'urn:li:[^']*'/g, '')
        .replace(/\s{2,}/g, ' ')
        .trim();
}

export function getChangeEventString(changeEvent: ChangeEvent) {
    let displayString = changeEvent.description;
    // Cast to string — the backend emits categories (DOMAIN, STRUCTURED_PROPERTY, APPLICATION)
    // not yet present in the generated ChangeCategoryType enum.
    const category = changeEvent.category as string;

    if (category === ChangeCategoryType.TechnicalSchema) {
        const fieldPath = getParameter(changeEvent.parameters, PARAM_FIELD_PATH, DEFAULT_FIELD_PATH);
        if (changeEvent.operation === ChangeOperationType.Add) {
            displayString = `Added column ${downgradeV2FieldPath(fieldPath || '')}.`;
        } else if (changeEvent.operation === ChangeOperationType.Remove) {
            displayString = `Removed column ${downgradeV2FieldPath(fieldPath || '')}.`;
        }
    } else if (category === ChangeCategoryType.Documentation) {
        const hasDescriptionParam = (changeEvent.parameters || []).some((p) => p.key === PARAM_DESCRIPTION);

        // Only override when a description parameter is present (EditableDatasetProperties events).
        // DomainProperties and GlossaryTermInfo events don't emit this parameter — fall through
        // to the backend description which already contains a human-readable message.
        if (hasDescriptionParam) {
            const description = getParameter(changeEvent.parameters, PARAM_DESCRIPTION, '');
            if (!changeEvent.modifier) {
                displayString = description === '' ? EMPTY_ASSET_DOC : SET_ASSET_DOC(description);
            } else {
                const fieldPath = changeEvent.modifier;
                displayString = description === '' ? EMPTY_FIELD_DOC(fieldPath) : SET_FIELD_DOC(fieldPath, description);
            }
        }
    } else if (category === ChangeCategoryType.Tag) {
        const tagUrn = getParameter(changeEvent.parameters, PARAM_TAG_URN, '');
        const tagName = tagUrn ? extractNameFromUrn(tagUrn) : 'Unknown';
        const fieldPath = getParameter(changeEvent.parameters, PARAM_FIELD_PATH);

        if (changeEvent.operation === ChangeOperationType.Add) {
            displayString = fieldPath
                ? `Added tag "${tagName}" to field ${downgradeV2FieldPath(fieldPath)}.`
                : `Added tag "${tagName}".`;
        } else if (changeEvent.operation === ChangeOperationType.Remove) {
            displayString = fieldPath
                ? `Removed tag "${tagName}" from field ${downgradeV2FieldPath(fieldPath)}.`
                : `Removed tag "${tagName}".`;
        }
    } else if (category === ChangeCategoryType.GlossaryTerm) {
        const termUrn = getParameter(changeEvent.parameters, PARAM_TERM_URN, '');
        const termName = termUrn ? extractNameFromUrn(termUrn) : 'Unknown';
        const fieldPath = getParameter(changeEvent.parameters, PARAM_FIELD_PATH);
        const relationshipType = getParameter(changeEvent.parameters, PARAM_RELATIONSHIP_TYPE);

        if (relationshipType) {
            const label = getRelationshipLabel(relationshipType);
            if (changeEvent.operation === ChangeOperationType.Add) {
                displayString = `Added ${label} term "${termName}".`;
            } else if (changeEvent.operation === ChangeOperationType.Remove) {
                displayString = `Removed ${label} term "${termName}".`;
            }
        } else if (termUrn) {
            if (changeEvent.operation === ChangeOperationType.Add) {
                displayString = fieldPath
                    ? `Added term "${termName}" to field ${downgradeV2FieldPath(fieldPath)}.`
                    : `Added term "${termName}".`;
            } else if (changeEvent.operation === ChangeOperationType.Remove) {
                displayString = fieldPath
                    ? `Removed term "${termName}" from field ${downgradeV2FieldPath(fieldPath)}.`
                    : `Removed term "${termName}".`;
            }
        }
    } else if (category === ChangeCategoryType.Ownership) {
        const ownerUrn = getParameter(changeEvent.parameters, PARAM_OWNER_URN, '');
        const ownerName = ownerUrn ? extractNameFromUrn(ownerUrn) : 'Unknown';
        const rawOwnerType = getParameter(changeEvent.parameters, PARAM_OWNER_TYPE);
        const ownerTypeUrn = getParameter(changeEvent.parameters, PARAM_OWNER_TYPE_URN);
        let ownerTypeSuffix = '';
        if (ownerTypeUrn) {
            ownerTypeSuffix = ` (${formatOwnerTypeUrn(ownerTypeUrn)})`;
        } else if (rawOwnerType && !UNINFORMATIVE_OWNER_TYPES.has(rawOwnerType)) {
            ownerTypeSuffix = ` (${formatOwnerType(rawOwnerType)})`;
        }

        if (changeEvent.operation === ChangeOperationType.Add) {
            displayString = `Added owner "${ownerName}"${ownerTypeSuffix}.`;
        } else if (changeEvent.operation === ChangeOperationType.Remove) {
            displayString = `Removed owner "${ownerName}"${ownerTypeSuffix}.`;
        }
    } else if (category === CATEGORY_DOMAIN) {
        const domainUrn = getParameter(changeEvent.parameters, PARAM_DOMAIN_URN, '');
        const domainName = domainUrn ? extractNameFromUrn(domainUrn) : 'Unknown';

        if (changeEvent.operation === ChangeOperationType.Add) {
            displayString = `Added to domain "${domainName}".`;
        } else if (changeEvent.operation === ChangeOperationType.Remove) {
            displayString = `Removed from domain "${domainName}".`;
        }
    } else if (category === CATEGORY_STRUCTURED_PROPERTY) {
        const propertyUrn = getParameter(changeEvent.parameters, PARAM_PROPERTY_URN, '');
        const propertyName = propertyUrn ? extractNameFromUrn(propertyUrn) : 'Unknown';
        const propertyValues = getParameter(changeEvent.parameters, PARAM_PROPERTY_VALUES);
        const valuesSuffix = propertyValues ? ` to ${formatPropertyValues(propertyValues)}` : '';

        if (changeEvent.operation === ChangeOperationType.Add) {
            displayString = `Set structured property "${propertyName}"${valuesSuffix}.`;
        } else if (changeEvent.operation === ChangeOperationType.Remove) {
            displayString = `Removed structured property "${propertyName}".`;
        } else if (changeEvent.operation === ChangeOperationType.Modify) {
            displayString = `Updated structured property "${propertyName}"${valuesSuffix}.`;
        }
    } else if (category === CATEGORY_APPLICATION) {
        const appUrn = changeEvent.modifier || '';
        const appName = appUrn ? extractNameFromUrn(appUrn) : 'Unknown';

        if (changeEvent.operation === ChangeOperationType.Add) {
            displayString = `Added to application "${appName}".`;
        } else if (changeEvent.operation === ChangeOperationType.Remove) {
            displayString = `Removed from application "${appName}".`;
        }
    }

    return stripEntityUrns(displayString);
}

/** @deprecated Use getChangeEventString instead */
export const getDocumentationString = getChangeEventString;
