import { StructuredPropertyEntity } from '@src/types.generated';

export function getDisplayName(structuredProperty: StructuredPropertyEntity) {
    return structuredProperty.definition.displayName || structuredProperty.definition.qualifiedName;
}
