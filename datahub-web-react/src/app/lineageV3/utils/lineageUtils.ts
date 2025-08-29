import { useLocation } from 'react-router-dom';

import { KEY_SCHEMA_PREFIX, VERSION_PREFIX } from '@app/entity/dataset/profile/schema/utils/constants';
import { getFieldPathFromSchemaFieldUrn } from '@app/entityV2/schemaField/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { EntityRegistry } from '@src/entityRegistryContext';

import { EntityType, SchemaField } from '@types';

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

export function getLineageUrl(
    urn: string,
    type: EntityType,
    location: ReturnType<typeof useLocation>,
    entityRegistry: EntityRegistry,
) {
    return `${entityRegistry.getEntityUrl(type, urn)}/Lineage${location.search}`;
}

export function useGetLineageUrl(urn?: string, type?: EntityType) {
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

    if (!urn || !type) {
        return '';
    }

    return getLineageUrl(urn, type, location, entityRegistry);
}

export function downloadImage(dataUrl: string, name?: string) {
    const now = new Date();
    const dateStr = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(
        now.getDate(),
    ).padStart(2, '0')}`;

    const timeStr = `${String(now.getHours()).padStart(2, '0')}${String(now.getMinutes()).padStart(2, '0')}${String(
        now.getSeconds(),
    ).padStart(2, '0')}`;

    const fileNamePrefix = name ? `${name}_` : 'reactflow_';
    const fileName = `${fileNamePrefix}${dateStr}_${timeStr}.png`;

    const a = document.createElement('a');
    a.setAttribute('download', fileName);
    a.setAttribute('href', dataUrl);
    a.click();
}
