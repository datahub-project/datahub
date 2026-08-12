import { useMemo } from 'react';

import { STRUCTURED_PROPERTY_FILTER_PREFIX } from '@app/entityV2/view/builder/constants';
import { viewBuilderProperties } from '@app/entityV2/view/builder/viewBuilderProperties';
import { DATE_TYPE_URN, NUMBER_TYPE_URN, URN_TYPE_URN } from '@app/shared/constants';
import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { SelectInputMode, SelectOption, ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';

import { useSearchStructuredPropertiesQuery } from '@graphql/structuredProperties.generated';
import { EntityType, StructuredPropertyEntity } from '@types';

// DataHub rarely defines more than a few dozen structured properties; fetch a generous
// page so every definition is offered as a filterable field without paginating.
const STRUCTURED_PROPERTY_FETCH_COUNT = 1000;

/**
 * Converts a structured property's allowed values into fixed select options.
 * Supports both string- and number-typed allowed values.
 */
function toAllowedValueOptions(entity: StructuredPropertyEntity): SelectOption[] {
    const allowedValues = entity.definition?.allowedValues ?? [];
    return allowedValues
        .map((allowed): SelectOption | undefined => {
            const { value } = allowed;
            if (value && 'stringValue' in value && value.stringValue != null) {
                return { id: value.stringValue, displayName: value.stringValue };
            }
            if (value && 'numberValue' in value && value.numberValue != null) {
                const stringified = String(value.numberValue);
                return { id: stringified, displayName: stringified };
            }
            return undefined;
        })
        .filter((option): option is SelectOption => !!option);
}

/**
 * Maps a structured property definition to a query-builder Property so it can be
 * selected and filtered on in the View builder. The property id is the same
 * `structuredProperties.<qualifiedName>` field name that search facets use, so the
 * resulting View filter round-trips through the existing predicate <-> filter conversion.
 */
export function structuredPropertyToViewProperty(entity: StructuredPropertyEntity): Property | undefined {
    const { definition } = entity;
    if (!definition?.qualifiedName) {
        return undefined;
    }

    const id = `${STRUCTURED_PROPERTY_FILTER_PREFIX}${definition.qualifiedName}`;
    const displayName = definition.displayName || definition.qualifiedName;
    const { description } = definition;
    const valueTypeUrn = definition.valueType?.urn;

    // URN-valued properties reference other entities — offer entity search, scoped to the
    // property's allowed entity types when the definition constrains them.
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

    // Properties with a constrained set of allowed values get a fixed multi-select.
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

    // Numeric and date properties (dates are indexed as epoch millis) support >, <, = filtering.
    if (valueTypeUrn === NUMBER_TYPE_URN || valueTypeUrn === DATE_TYPE_URN) {
        return { id, displayName, description: description || undefined, valueType: ValueTypeId.NUMBER };
    }

    // Free-form string / rich-text (and anything unrecognised) fall back to a text input.
    return { id, displayName, description: description || undefined, valueType: ValueTypeId.STRING };
}

/**
 * Returns the list of properties available in the View builder's Build Filters tab:
 * the static, well-known fields plus every structured property defined in the instance.
 */
export function useViewBuilderProperties(): Property[] {
    const { data } = useSearchStructuredPropertiesQuery({
        variables: { query: '*', start: 0, count: STRUCTURED_PROPERTY_FETCH_COUNT },
        fetchPolicy: 'cache-first',
    });

    return useMemo(() => {
        const results = data?.searchAcrossEntities?.searchResults ?? [];
        const structuredPropertyProperties = (results.map((result) => result.entity) as StructuredPropertyEntity[])
            .map(structuredPropertyToViewProperty)
            .filter((property): property is Property => !!property);
        return [...viewBuilderProperties, ...structuredPropertyProperties];
    }, [data]);
}
