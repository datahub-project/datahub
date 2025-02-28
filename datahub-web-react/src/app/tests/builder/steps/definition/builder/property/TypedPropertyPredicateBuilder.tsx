import React from 'react';
import styled from 'styled-components';
import { PropertyPredicate } from '../types';
import { Property } from './types/properties';
import { getValueOptions, getOperatorOptions, getPropertyById } from './utils';
import { PropertyTreeSelect } from './select/PropertyTreeSelect';
import { OperatorSelect } from './select/OperatorSelect';
import { ValueSelect } from './select/ValueSelect';

const PredicateContainer = styled.div`
    display: flex;
    align-items: center;
`;

type Props = {
    selectedPredicate?: PropertyPredicate;
    properties: Property[];
    onChangeProperty: (newPropertyId: string) => void;
    onChangeOperator: (newOperatorId: string) => void;
    onChangeValues: (newOperatorValues: string[]) => void;
};

/**
 * This component allows you to construct a single Typed (well-supported) Property Predicate.
 *
 * This differs from a custom property predicate in that we formally support the property
 * on the UI, making it much easier for the end user to use it.
 */
export const TypedPropertyPredicateBuilder = ({
    selectedPredicate,
    properties,
    onChangeProperty,
    onChangeOperator,
    onChangeValues,
}: Props) => {
    /**
     * Retrieve the selected property to render.
     */
    const property =
        (selectedPredicate && selectedPredicate.property && getPropertyById(selectedPredicate.property, properties)) ||
        undefined;
    /**
     * Get the operators that can be applied to the current field.
     */
    const operatorOptions = (property?.valueType && getOperatorOptions(property.valueType)) || undefined;

    /**
     * Get options required for rendering the "values" input. This is a function of the selected property and
     * operator.
     */
    const valueOptions = (property && selectedPredicate && getValueOptions(property, selectedPredicate)) || undefined;

    return (
        <PredicateContainer>
            <PropertyTreeSelect
                selectedProperty={selectedPredicate?.property}
                properties={properties}
                onChangeProperty={onChangeProperty}
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
