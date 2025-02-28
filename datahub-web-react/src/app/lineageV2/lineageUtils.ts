import { useEntityRegistry } from '@app/useEntityRegistry';
import { useLocation } from 'react-router-dom';
import { EntityType, SchemaField } from '../../types.generated';
import { KEY_SCHEMA_PREFIX, VERSION_PREFIX } from '../entity/dataset/profile/schema/utils/constants';
import EntityRegistry from '../entityV2/EntityRegistry';
import { getFieldPathFromSchemaFieldUrn } from '../entityV2/schemaField/utils';

export function downgradeV2FieldPath(fieldPath: string): string;
export function downgradeV2FieldPath(fieldPath?: string | null) {
    if (!fieldPath) {
        return fieldPath;
    }

    const cleanedFieldPath = fieldPath.replace(KEY_SCHEMA_PREFIX, '').replace(VERSION_PREFIX, '');

    // strip out all annotation segments
    return cleanedFieldPath
        .split('.')
        .map((segment) => (segment.startsWith('[') ? null : segment))
        .filter(Boolean)
        .join('.');
}

export function processDocumentationString(docString): string {
    if (!docString) {
        return '';
    }
    const fieldRegex = /'(\[version=2\.0\](?:\.\[key=True\])?\.\[type=[^\]]+\]\.[^']+)'/g;
    return docString.replace(fieldRegex, (_, fieldPath) => `'${downgradeV2FieldPath(fieldPath)}'`);
}

export function convertFieldsToV1FieldPath(fields: SchemaField[]) {
    return fields.map((field) => ({
        ...field,
        fieldPath: downgradeV2FieldPath(field.fieldPath) || '',
    }));
}

export function getV1FieldPathFromSchemaFieldUrn(schemaFieldUrn: string) {
    return downgradeV2FieldPath(getFieldPathFromSchemaFieldUrn(schemaFieldUrn)) as string;
}

export function getEntityTypeFromEntityUrn(urn: string, registry: EntityRegistry): EntityType | undefined {
    const [, , entityType] = urn.split(':');
    return registry.getTypeFromGraphName(entityType);
}

const PLATFORM_URN_TYPES = ['dataset', 'mlModel', 'mlModelGroup', 'mlFeatureTable'];
const TRUNCATED_PLATFORM_TYPES = ['dataFlow', 'dataJob', 'chart', 'dashboard'];
const NESTED_URN_TYPES = ['schemaField'];

// TODO: Add tests
export function getPlatformUrnFromEntityUrn(urn: string): string | undefined {
    const [, , entityType, ...entityIdsStr] = urn.split(':');
    const entityIds = splitEntityId(entityIdsStr.join(':'));
    if (PLATFORM_URN_TYPES.includes(entityType)) {
        return entityIds[0];
    }
    if (TRUNCATED_PLATFORM_TYPES.includes(entityType)) {
        return `urn:li:dataPlatform:${entityIds[0]}`;
    }
    if (NESTED_URN_TYPES.includes(entityType)) {
        return getPlatformUrnFromEntityUrn(entityIds[0]);
    }
    return undefined;
}

/** Mimics metadata-ingestion function in _urn_base.py */
function splitEntityId(entity_id: string): string[] {
    if (!(entity_id.startsWith('(') && entity_id.endsWith(')'))) {
        return [entity_id];
    }
    const parts: string[] = [];
    let startParentCount = 1;
    let partStart = 1;
    for (let i = 1; i < entity_id.length; i++) {
        const c = entity_id[i];
        if (c === '(') {
            startParentCount += 1;
        } else if (c === ')') {
            startParentCount -= 1;
            if (startParentCount < 0) {
                throw new Error(`${entity_id}, mismatched paren nesting`);
            }
        } else if (c === ',' && startParentCount === 1) {
            if (i - partStart <= 0) {
                throw new Error(`${entity_id}, empty part disallowed`);
            }
            parts.push(entity_id.slice(partStart, i));
            partStart = i + 1;
        }
    }

    if (startParentCount !== 0) {
        throw new Error(`${entity_id}, mismatched paren nesting`);
    }

    parts.push(entity_id.slice(partStart, -1));

    return parts;
}

export function useGetLineageUrl(urn?: string, type?: EntityType) {
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

    if (!urn || !type) {
        return '';
    }
    return `${entityRegistry.getEntityUrl(type, urn)}/Lineage${location.search}`;
}
