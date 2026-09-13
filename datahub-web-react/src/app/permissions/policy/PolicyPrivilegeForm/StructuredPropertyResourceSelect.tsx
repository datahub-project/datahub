import { Button, SimpleSelect } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import ConditionSelectDropdown from '@app/permissions/policy/ConditionSelectDropdown';
import ValueInput from '@app/permissions/policy/PolicyPrivilegeForm/ValueInput';
import { useClearOnConditionChange } from '@app/permissions/policy/PolicyPrivilegeForm/useClearOnConditionChange';
import { FIELD_TYPES } from '@app/permissions/policy/constants';
import { URN_TYPE_URN } from '@app/shared/constants';

import { useSearchStructuredPropertiesQuery } from '@graphql/structuredProperties.generated';
import { EntityType, PolicyMatchCondition, ResourceFilter } from '@types';

type StructuredPropertyValue = {
    propertyUrn: string;
    values: string[];
};

interface StructuredPropertyDefinition {
    urn: string;
    definition?: {
        displayName?: string;
        valueType?: { urn?: string };
        allowedValues?: Array<{ value?: { stringValue?: string; numberValue?: number }; description?: string }>;
        typeQualifier?: { allowedTypes?: Array<{ type?: string; info?: { type?: string } }> };
    };
}

type Props = {
    structuredProperties: StructuredPropertyValue[];
    condition: PolicyMatchCondition;
    onConditionChange: (condition: PolicyMatchCondition, updatedResources: ResourceFilter) => void;
    onStructuredPropertiesChange: (properties: StructuredPropertyValue[]) => void;
    resources: ResourceFilter;
};

// Helper: Check if any structured properties are incomplete
export const hasIncompleteStructuredProperties = (properties: StructuredPropertyValue[]): boolean => {
    return properties.some(
        (prop) =>
            // Property URN is provided and non-empty after trimming
            (prop.propertyUrn?.trim()?.length ?? 0) > 0 &&
            // But values array is missing, not an array, or empty
            (!Array.isArray(prop.values) || prop.values.length === 0),
    );
};

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    width: 100%;
`;

const MainRow = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
    width: 100%;
`;

const ConditionSelect = styled.div`
    flex-shrink: 0;
`;

const PropertyRowsContainer = styled.div<{ $hasRows: boolean }>`
    display: flex;
    flex-direction: column;
    gap: 8px;
    ${(props) => props.$hasRows && 'flex: 1; min-width: 0;'}
`;

const PropertyRow = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
    width: 100%;
`;

const PropertySelect = styled.div`
    flex: 1;
    min-width: 0;
`;

const ValueInputContainer = styled.div`
    flex: 1;
    min-width: 0;
`;

const DeleteButton = styled(Button)`
    flex-shrink: 0;
`;

const AddPropertyButton = styled(Button)`
    width: fit-content;
    flex-shrink: 0;
`;

export default function StructuredPropertyResourceSelect({
    structuredProperties,
    condition,
    onConditionChange,
    onStructuredPropertiesChange,
    resources,
}: Props) {
    const { t } = useTranslation('settings.permissions');
    const initializedRef = React.useRef(false);
    const [searchQuery, setSearchQuery] = useState('');
    const definitionCacheRef = React.useRef<Map<string, StructuredPropertyDefinition>>(new Map());

    const { data: propertiesData } = useSearchStructuredPropertiesQuery({
        variables: { query: searchQuery, start: 0, count: 100 },
        fetchPolicy: 'cache-and-network',
    });

    // Initialize with one empty row on first mount if structuredProperties is empty
    useEffect(() => {
        if (!initializedRef.current && structuredProperties.length === 0) {
            initializedRef.current = true;
            onStructuredPropertiesChange([{ propertyUrn: '', values: [] }]);
        }
    }, [structuredProperties.length, onStructuredPropertiesChange]);

    const propertyDefinitions = useMemo(() => {
        const results = propertiesData?.searchAcrossEntities?.searchResults || [];
        const searchResultDefs = results
            .map((result) => result.entity)
            .filter((entity) => entity?.__typename === 'StructuredPropertyEntity') as StructuredPropertyDefinition[];

        // Update cache with new search results
        searchResultDefs.forEach((def) => {
            if (def?.urn) {
                definitionCacheRef.current.set(def.urn, def);
            }
        });

        // Return cache + new results (cache ensures selected properties remain available after search)
        return Array.from(definitionCacheRef.current.values());
    }, [propertiesData]);

    const propertyOptions = useMemo(() => {
        const optionsMap = new Map<string, { value: string; label: string }>();

        // Add all definitions from search results
        propertyDefinitions.forEach((entity) => {
            const ent = entity as StructuredPropertyDefinition;
            optionsMap.set(ent.urn, {
                value: ent.urn,
                label: ent.definition?.displayName || ent.urn,
            });
        });

        // Ensure already-selected properties are in the options
        structuredProperties.forEach((prop) => {
            if (prop.propertyUrn && !optionsMap.has(prop.propertyUrn)) {
                optionsMap.set(prop.propertyUrn, {
                    value: prop.propertyUrn,
                    label: prop.propertyUrn,
                });
            }
        });

        return Array.from(optionsMap.values());
    }, [propertyDefinitions, structuredProperties]);

    const getPropertyDefinition = (urn: string): StructuredPropertyDefinition | undefined => {
        return propertyDefinitions.find((e) => {
            const ent = e as StructuredPropertyDefinition;
            return ent.urn === urn;
        }) as StructuredPropertyDefinition | undefined;
    };

    const getValueType = (urn: string) => {
        return getPropertyDefinition(urn)?.definition?.valueType?.urn;
    };

    const getValueOptions = (propertyUrn: string) => {
        const definition = getPropertyDefinition(propertyUrn)?.definition;
        // For string, number, and date types, return allowedValues (if defined)
        return definition?.allowedValues || [];
    };

    const getEntityTypes = (propertyUrn: string): EntityType[] => {
        const definition = getPropertyDefinition(propertyUrn)?.definition;
        const valueTypeUrn = definition?.valueType?.urn;

        // For URN types, extract entity types from the allowed types
        if (valueTypeUrn === URN_TYPE_URN && definition?.typeQualifier?.allowedTypes) {
            return (definition.typeQualifier.allowedTypes ?? [])
                .map((allowedType) => (allowedType as any)?.info?.type)
                .filter((type): type is string => !!type)
                .map((type) => type as EntityType);
        }

        return [];
    };

    const handlePropertyChange = (index: number, propertyUrn: string) => {
        const updated = structuredProperties.map((prop, i) =>
            i === index ? { ...prop, propertyUrn, values: [] } : prop,
        );
        onStructuredPropertiesChange(updated);
    };

    const handleValueChange = (index: number, values: string[]) => {
        const updated = structuredProperties.map((prop, i) => (i === index ? { ...prop, values } : prop));
        onStructuredPropertiesChange(updated);
    };

    const handleDeleteRow = (index: number) => {
        const updated = structuredProperties.filter((_, i) => i !== index);
        onStructuredPropertiesChange(updated);
    };

    const handleAddProperty = () => {
        const updated = [...structuredProperties, { propertyUrn: '', values: [] }];
        onStructuredPropertiesChange(updated);
    };

    const handleConditionChange = useClearOnConditionChange(
        condition,
        FIELD_TYPES.STRUCTURED_PROPERTY,
        onConditionChange,
    );

    const hasValues = useMemo(
        () => structuredProperties.some((p) => p.propertyUrn && p.values.length > 0),
        [structuredProperties],
    );

    return (
        <Container>
            <MainRow>
                <ConditionSelect>
                    <ConditionSelectDropdown
                        condition={condition}
                        onConditionChange={handleConditionChange}
                        fieldType={FIELD_TYPES.STRUCTURED_PROPERTY}
                        hasValues={hasValues}
                        resources={resources}
                    />
                </ConditionSelect>

                <PropertyRowsContainer $hasRows={structuredProperties.length > 0}>
                    {structuredProperties.map((prop, index) => {
                        const propertyKey = prop.propertyUrn ? `${prop.propertyUrn}-${index}` : `empty-${index}`;

                        return (
                            <PropertyRow key={propertyKey} data-testid={`property-row-${propertyKey}`}>
                                <PropertySelect>
                                    <SimpleSelect
                                        options={propertyOptions}
                                        values={prop.propertyUrn ? [prop.propertyUrn] : []}
                                        onUpdate={(selected: string[]) =>
                                            handlePropertyChange(index, selected?.[0] || '')
                                        }
                                        placeholder={t('privilegeForm.selectProperty')}
                                        isMultiSelect={false}
                                        showClear={false}
                                        width="full"
                                        showSearch
                                        onSearchChange={setSearchQuery}
                                    />
                                </PropertySelect>
                                {prop.propertyUrn && (
                                    <ValueInputContainer>
                                        <ValueInput
                                            values={prop.values}
                                            valueTypeUrn={getValueType(prop.propertyUrn)}
                                            allowedValues={getValueOptions(prop.propertyUrn)}
                                            allowedEntityTypes={getEntityTypes(prop.propertyUrn)}
                                            onUpdate={(newValues) => handleValueChange(index, newValues)}
                                        />
                                    </ValueInputContainer>
                                )}
                                <DeleteButton variant="text" color="gray" onClick={() => handleDeleteRow(index)}>
                                    <Trash />
                                </DeleteButton>
                            </PropertyRow>
                        );
                    })}
                </PropertyRowsContainer>

                <AddPropertyButton variant="text" icon={{ icon: Plus }} onClick={handleAddProperty}>
                    {t('privilegeForm.addProperty')}
                </AddPropertyButton>
            </MainRow>
        </Container>
    );
}
