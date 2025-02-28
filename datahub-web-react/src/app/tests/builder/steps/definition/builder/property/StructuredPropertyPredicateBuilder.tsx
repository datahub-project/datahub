import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { PropertyPredicate } from '../types';
import {
    extractStructuredPropertyReferenceUrn,
    getStructuredPropertiesOperatorOptions,
    getStructuredPropertyValueOptions,
} from './utils';
import { OperatorSelect } from './select/OperatorSelect';
import { ValueSelect } from './select/ValueSelect';
import { StructuredPropertyEntity } from '../../../../../../../types.generated';
import { StructuredPropertySelect } from './select/structured/StructuredPropertySelect';
import { useGetStructuredPropertyLazyQuery } from '../../../../../../../graphql/structuredProperties.generated';
import { STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID } from './constants';

const PredicateContainer = styled.div`
    display: flex;
    align-items: center;
`;

type Props = {
    selectedPredicate?: PropertyPredicate;
    onChangeProperty: (newPropertyId?: string) => void;
    onChangeOperator: (newOperatorId?: string) => void;
    onChangeValues: (newOperatorValues?: string[]) => void;
};

/**
 * This component allows you to construct a predicate for a specific structured property.
 */
export const StructuredPropertyPredicateBuilder = ({
    selectedPredicate,
    onChangeProperty,
    onChangeOperator,
    onChangeValues,
}: Props) => {
    const selectedPropertyUrn =
        (selectedPredicate?.property && extractStructuredPropertyReferenceUrn(selectedPredicate?.property)) ||
        undefined;

    // If a property is selected, we look up the property.
    const [resolvedPropertyDefinition, setResolvedPropertyDefinition] = useState<StructuredPropertyEntity | null>(null);

    /**
     * Bootstrap component by resolving the definition of a the currently selected property (if there is one.)
     */
    const [getEntity, { data: resolvedEntityData }] = useGetStructuredPropertyLazyQuery();
    useEffect(() => {
        if (!resolvedPropertyDefinition && selectedPropertyUrn) {
            // Resolve urns to their full entities
            getEntity({ variables: { urn: selectedPropertyUrn } });
        }
    }, [selectedPropertyUrn, resolvedPropertyDefinition, getEntity]);

    /**
     * Once the property definition data has been resolved, simply populate the local
     * state with the value, so we can use elsewhere.
     */
    useEffect(() => {
        if (resolvedEntityData && resolvedEntityData.entity) {
            const entity: StructuredPropertyEntity = (resolvedEntityData?.entity as StructuredPropertyEntity) || [];
            setResolvedPropertyDefinition(entity);
        }
    }, [resolvedEntityData]);

    /**
     * The operator options depend on the structured property that is selected.
     * For now, we'll hardcode this to be basic equivalence properties.
     * List vs string vs number vs date vs boolean will have their own operators based
     * on the type of the property.
     */
    const operatorOptions =
        (resolvedPropertyDefinition && getStructuredPropertiesOperatorOptions(resolvedPropertyDefinition)) || undefined;

    /**
     * Get options required for rendering the options input once a structured property has been selected.
     *
     * This dictates the authoring experience, and again will be based on the property + operator
     * selected.
     *
     * For now, we'll assume that structured properties have only a multi-text input.
     * This Depends
     */
    const valueOptions =
        (resolvedPropertyDefinition && getStructuredPropertyValueOptions(resolvedPropertyDefinition)) || undefined;

    /**
     * When a structured property is selected, we simply
     * create a new metadata test property that acts as a reference to that property,
     * of the form: structuredProperties.urn:li:structuredProperty:xyz
     */
    const onSelectProperty = (newProperty) => {
        if (newProperty) {
            const newPropertyId = `structuredProperties.${newProperty.urn}`;
            onChangeProperty(newPropertyId);
            setResolvedPropertyDefinition(newProperty);
        } else {
            onChangeProperty(STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID);
            setResolvedPropertyDefinition(null);
        }
    };

    return (
        <PredicateContainer>
            {/** This should be informed by the entity type! */}
            <StructuredPropertySelect
                selectedProperty={resolvedPropertyDefinition || undefined}
                onSelect={onSelectProperty}
                onClear={() => onSelectProperty(undefined)}
            />
            {operatorOptions && (
                <OperatorSelect
                    selectedOperator={selectedPredicate?.operator}
                    operators={operatorOptions}
                    onChangeOperator={onChangeOperator}
                />
            )}
            {valueOptions && (
                <ValueSelect
                    selectedValues={selectedPredicate?.values}
                    options={valueOptions}
                    onChangeValues={onChangeValues}
                />
            )}
        </PredicateContainer>
    );
};
