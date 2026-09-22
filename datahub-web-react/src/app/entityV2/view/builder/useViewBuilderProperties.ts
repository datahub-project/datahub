import i18next from 'i18next';
import { useMemo } from 'react';

import { STRUCTURED_PROPERTY_FILTER_PREFIX } from '@app/entityV2/view/builder/constants';
import { viewBuilderProperties } from '@app/entityV2/view/builder/viewBuilderProperties';
import { DATE_TYPE_URN, NUMBER_TYPE_URN, URN_TYPE_URN } from '@app/shared/constants';
import { STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID } from '@app/sharedV2/queryBuilder/builder/property/constants';
import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { SelectInputMode, SelectOption, ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';

import { useSearchStructuredPropertiesQuery } from '@graphql/structuredProperties.generated';
import { EntityType, StructuredPropertyEntity } from '@types';

const STRUCTURED_PROPERTY_FETCH_COUNT = 1000;

function toAllowedValueOptions(entity: StructuredPropertyEntity): SelectOption[] {
    const allowedValues = entity.definition?.allowedValues ?? [];
    return allowedValues
        .map((allowed): SelectOption | undefined => {
            const { value, description } = allowed;
            // The filter stores the raw value (id), but the picker shows the human-friendly
            // label the property author set on the allowed value, falling back to the value itself.
            if (value && 'stringValue' in value && value.stringValue != null) {
                return { id: value.stringValue, displayName: description || value.stringValue };
            }
            if (value && 'numberValue' in value && value.numberValue != null) {
                const stringified = String(value.numberValue);
                return { id: stringified, displayName: description || stringified };
            }
            return undefined;
        })
        .filter((option): option is SelectOption => !!option);
}

// The property id is the `structuredProperties.<qualifiedName>` field that search facets use,
// so the resulting View filter round-trips through the existing predicate <-> filter conversion.
export function structuredPropertyToViewProperty(entity: StructuredPropertyEntity): Property | undefined {
    const { definition } = entity;
    if (!definition?.qualifiedName) {
        return undefined;
    }

    const id = `${STRUCTURED_PROPERTY_FILTER_PREFIX}${definition.qualifiedName}`;
    const displayName = definition.displayName || definition.qualifiedName;
    const { description } = definition;
    const valueTypeUrn = definition.valueType?.urn;

    if (valueTypeUrn === URN_TYPE_URN) {
        const entityTypes = (definition.typeQualifier?.allowedTypes ?? [])
            .map((allowedType) => allowedType.type)
            .filter((type): type is EntityType => !!type);
        return {
            id,
            displayName,
            description: description || undefined,
            valueType: ValueTypeId.URN,
            valueOptions: { entityTypes, mode: SelectInputMode.MULTIPLE },
        };
    }

    const allowedValueOptions = toAllowedValueOptions(entity);
    if (allowedValueOptions.length > 0) {
        return {
            id,
            displayName,
            description: description || undefined,
            valueType: ValueTypeId.ENUM,
            valueOptions: { options: allowedValueOptions, mode: SelectInputMode.MULTIPLE },
        };
    }

    if (valueTypeUrn === NUMBER_TYPE_URN) {
        return { id, displayName, description: description || undefined, valueType: ValueTypeId.NUMBER };
    }

    // Dates are indexed as epoch millis, but TIMESTAMP renders a date picker (that stores millis)
    // rather than a numeric field that would force the user to type epoch millis by hand.
    if (valueTypeUrn === DATE_TYPE_URN) {
        return { id, displayName, description: description || undefined, valueType: ValueTypeId.TIMESTAMP };
    }

    return { id, displayName, description: description || undefined, valueType: ValueTypeId.STRING };
}

// Nests the structured properties under one parent entry so the user picks
// "Structured Property" first and then the specific property, rather than
// scrolling a flat list of every definition alongside the static fields.
export function buildViewBuilderProperties(entities: StructuredPropertyEntity[]): Property[] {
    const structuredPropertyProperties = entities
        .map(structuredPropertyToViewProperty)
        .filter((property): property is Property => !!property);

    if (structuredPropertyProperties.length === 0) {
        return viewBuilderProperties;
    }

    const structuredPropertyGroup: Property = {
        id: STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
        get displayName() {
            return i18next.t('entity.views:prop.structuredPropertyGroup');
        },
        children: structuredPropertyProperties,
    };
    return [...viewBuilderProperties, structuredPropertyGroup];
}

export function useViewBuilderProperties(): Property[] {
    const { data } = useSearchStructuredPropertiesQuery({
        variables: { query: '*', start: 0, count: STRUCTURED_PROPERTY_FETCH_COUNT },
        fetchPolicy: 'cache-first',
    });

    return useMemo(() => {
        const entities = (data?.searchAcrossEntities?.searchResults ?? []).map(
            (result) => result.entity,
        ) as StructuredPropertyEntity[];
        return buildViewBuilderProperties(entities);
    }, [data]);
}
