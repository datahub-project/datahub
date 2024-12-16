import { getStructuredPropertyValue } from '../../../utils';
import { GenericEntityProperties } from '../../../types';
import {
    Entity,
    FormPrompt,
    PropertyValue,
    SchemaField,
    StructuredPropertiesEntry,
} from '../../../../../../types.generated';

export function getInitialValues(prompt: FormPrompt, entityData: GenericEntityProperties | null, field?: SchemaField) {
    const structuredProperty = prompt.structuredPropertyParams?.structuredProperty;
    let structuredPropertyAssignment: StructuredPropertiesEntry | undefined;
    if (field) {
        structuredPropertyAssignment = field?.schemaFieldEntity?.structuredProperties?.properties?.find(
            (propAssignment) => propAssignment.structuredProperty.urn === structuredProperty?.urn,
        );
    } else {
        structuredPropertyAssignment = entityData?.structuredProperties?.properties?.find(
            (propAssignment) => propAssignment.structuredProperty.urn === structuredProperty?.urn,
        );
    }
    return structuredPropertyAssignment?.values?.map((value) => getStructuredPropertyValue(value as PropertyValue));
}

export function getInitialEntitiesForUrnPrompt(
    structuredPropertyUrn: string,
    entityData: GenericEntityProperties | null,
    selectedValues: any[],
) {
    const structuredPropertyEntry = entityData?.structuredProperties?.properties?.find(
        (p) => p.structuredProperty.urn === structuredPropertyUrn,
    );
    const entities = structuredPropertyEntry?.valueEntities?.filter((e) => selectedValues.includes(e?.urn));
    return entities ? (entities as Entity[]) : [];
}
