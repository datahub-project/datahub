import EntityRegistry from '../entity/EntityRegistry';
import { Entity, EntityType } from '../../types.generated';

export function sortBusinessAttributes(entityRegistry: EntityRegistry, nodeA?: Entity | null, nodeB?: Entity | null) {
    const nodeAName = entityRegistry.getDisplayName(EntityType.BusinessAttribute, nodeA) || '';
    const nodeBName = entityRegistry.getDisplayName(EntityType.BusinessAttribute, nodeB) || '';
    return nodeAName.localeCompare(nodeBName);
}

export function getRelatedEntitiesUrl(entityRegistry: EntityRegistry, urn: string) {
    return `${entityRegistry.getEntityUrl(EntityType.BusinessAttribute, urn)}/${encodeURIComponent(
        'Related Entities',
    )}`;
}

export enum SchemaFieldDataType {
    /** A boolean type */
    Boolean = 'BOOLEAN',
    /** A fixed bytestring type */
    Fixed = 'FIXED',
    /** A string type */
    String = 'STRING',
    /** A string of bytes */
    Bytes = 'BYTES',
    /** A number, including integers, floats, and doubles */
    Number = 'NUMBER',
    /** A datestrings type */
    Date = 'DATE',
    /** A timestamp type */
    Time = 'TIME',
    /** An enum type */
    Enum = 'ENUM',
    /** A map collection type */
    Map = 'MAP',
    /** An array collection type */
    Array = 'ARRAY',
}
