import { URN_TYPE_URN } from '@app/shared/constants';

import { EntityType } from '@types';

export type StructuredPropertyValue = {
    propertyUrn: string;
    values: string[];
};

export interface StructuredPropertyDefinition {
    urn: string;
    definition?: {
        displayName?: string;
        valueType?: { urn?: string };
        allowedValues?: Array<{ value?: { stringValue?: string; numberValue?: number }; description?: string }>;
        typeQualifier?: { allowedTypes?: Array<{ type?: string; info?: { type?: string } }> };
    };
}

export type PropertyOption = {
    value: string;
    label: string;
};

export const hasIncompleteStructuredProperties = (properties: StructuredPropertyValue[]): boolean => {
    return properties.some(
        (prop) =>
            (prop.propertyUrn?.trim()?.length ?? 0) > 0 && (!Array.isArray(prop.values) || prop.values.length === 0),
    );
};

export const getValueType = (definition: StructuredPropertyDefinition | undefined): string | undefined => {
    return definition?.definition?.valueType?.urn;
};

export const getValueOptions = (
    definition: StructuredPropertyDefinition | undefined,
): Array<{ value?: { stringValue?: string; numberValue?: number }; description?: string }> => {
    return definition?.definition?.allowedValues || [];
};

export const getEntityTypes = (definition: StructuredPropertyDefinition | undefined): EntityType[] => {
    const valueTypeUrn = definition?.definition?.valueType?.urn;

    if (valueTypeUrn === URN_TYPE_URN && definition?.definition?.typeQualifier?.allowedTypes) {
        return (definition.definition.typeQualifier.allowedTypes ?? [])
            .map((allowedType) => (allowedType as any)?.info?.type)
            .filter((type): type is string => !!type)
            .map((type) => type as EntityType);
    }

    return [];
};

export const buildPropertyOptionsMap = (
    searchResults: StructuredPropertyDefinition[],
    selectedPropertyData: StructuredPropertyDefinition | undefined,
    selectedPropertyUrn: string | undefined,
): Map<string, PropertyOption> => {
    const optionsMap = new Map<string, PropertyOption>();

    // Add search results
    searchResults.forEach((entity) => {
        if (entity?.urn) {
            optionsMap.set(entity.urn, {
                value: entity.urn,
                label: entity.definition?.displayName || entity.urn,
            });
        }
    });

    // Add fetched selected property if available
    if (selectedPropertyData?.urn) {
        optionsMap.set(selectedPropertyData.urn, {
            value: selectedPropertyData.urn,
            label: selectedPropertyData.definition?.displayName || selectedPropertyData.urn,
        });
    }

    // Add selected property placeholders not yet fetched
    if (selectedPropertyUrn && !optionsMap.has(selectedPropertyUrn)) {
        optionsMap.set(selectedPropertyUrn, {
            value: selectedPropertyUrn,
            label: selectedPropertyUrn,
        });
    }

    return optionsMap;
};

export const buildDefinitionsMap = (propertyOptions: PropertyOption[]): Map<string, StructuredPropertyDefinition> => {
    const map = new Map<string, StructuredPropertyDefinition>();
    propertyOptions.forEach((opt) => {
        const def = propertyOptions.find((o) => o.value === opt.value);
        if (def) {
            map.set(def.value, {
                urn: def.value,
                definition: { displayName: def.label !== def.value ? def.label : undefined },
            } as StructuredPropertyDefinition);
        }
    });
    return map;
};
