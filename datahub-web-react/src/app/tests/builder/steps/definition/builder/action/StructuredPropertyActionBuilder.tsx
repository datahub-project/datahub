import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { Action } from '@app/tests/builder/steps/actions/types';
import { StructuredPropertyValueInput } from '@app/tests/builder/steps/definition/builder/action/input/StructuredPropertyValueInput';
import { StructuredPropertySelect } from '@app/tests/builder/steps/definition/builder/property/select/structured/StructuredPropertySelect';
import { ActionId } from '@app/tests/builder/steps/definition/builder/property/types/action';

import { useGetStructuredPropertyLazyQuery } from '@graphql/structuredProperties.generated';
import { EntityType, StructuredPropertyEntity } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 8px;
`;

interface Props {
    selectedAction: Action;
    onChangeAction: (action: Action) => void;
    entityTypes: EntityType[];
}

export const StructuredPropertyActionBuilder = ({ selectedAction, onChangeAction, entityTypes }: Props) => {
    const [selectedProperty, setSelectedProperty] = useState<StructuredPropertyEntity | null>(null);
    const [propertyValues, setPropertyValues] = useState<string[]>([]);

    const isUnsetAction = selectedAction.type === ActionId.UNSET_STRUCTURED_PROPERTY;

    // Lazy query to fetch structured property by URN
    const [getStructuredProperty, { data: structuredPropertyData }] = useGetStructuredPropertyLazyQuery();

    // Initialize from existing action values
    useEffect(() => {
        if (selectedAction.values && selectedAction.values.length > 0) {
            // First value is the structured property URN
            const propertyUrn = selectedAction.values[0];

            // Fetch the structured property entity by URN to populate selectedProperty
            // Only fetch if we don't have the property or if the URN has changed
            if (propertyUrn && (!selectedProperty || selectedProperty.urn !== propertyUrn)) {
                getStructuredProperty({ variables: { urn: propertyUrn } });
            }

            // Remaining values are the property values (for SET action)
            if (!isUnsetAction && selectedAction.values.length > 1) {
                setPropertyValues(selectedAction.values.slice(1));
            }
        } else {
            // Clear state when no values are present
            setSelectedProperty(null);
            setPropertyValues([]);
        }
    }, [selectedAction.values, isUnsetAction, selectedProperty, getStructuredProperty]);

    // Handle structured property data response
    useEffect(() => {
        if (structuredPropertyData?.entity) {
            const entity = structuredPropertyData.entity as StructuredPropertyEntity;
            setSelectedProperty(entity);
        }
    }, [structuredPropertyData]);

    const handlePropertySelect = (property: StructuredPropertyEntity | undefined) => {
        setSelectedProperty(property || null);

        if (property) {
            // Update action with structured property URN
            const newValues = [property.urn];
            if (!isUnsetAction && propertyValues.length > 0) {
                newValues.push(...propertyValues);
            }

            onChangeAction({
                ...selectedAction,
                values: newValues,
            });
        } else {
            // Clear action values
            onChangeAction({
                ...selectedAction,
                values: [],
            });
        }
    };

    const handlePropertyClear = () => {
        setSelectedProperty(null);
        setPropertyValues([]);
        onChangeAction({
            ...selectedAction,
            values: [],
        });
    };

    const handleValuesChange = (values: string[]) => {
        setPropertyValues(values);

        if (selectedProperty) {
            const newActionValues = [selectedProperty.urn];
            if (!isUnsetAction && values.length > 0) {
                newActionValues.push(...values);
            }

            onChangeAction({
                ...selectedAction,
                values: newActionValues,
            });
        }
    };

    return (
        <Container>
            <StructuredPropertySelect
                selectedProperty={selectedProperty || undefined}
                entityTypes={entityTypes}
                onSelect={handlePropertySelect}
                onClear={handlePropertyClear}
            />
            <StructuredPropertyValueInput
                structuredProperty={selectedProperty}
                selectedValues={propertyValues}
                onChangeValues={handleValuesChange}
                isUnsetAction={isUnsetAction}
            />
        </Container>
    );
};
