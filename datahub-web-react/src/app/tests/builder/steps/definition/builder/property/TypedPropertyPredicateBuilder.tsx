import React from 'react';
import styled from 'styled-components';

import { OperatorSelect } from '@app/tests/builder/steps/definition/builder/property/select/OperatorSelect';
import { PropertyTreeSelect } from '@app/tests/builder/steps/definition/builder/property/select/PropertyTreeSelect';
import { ValueSelect } from '@app/tests/builder/steps/definition/builder/property/select/ValueSelect';
import { Property } from '@app/tests/builder/steps/definition/builder/property/types/properties';
import {
    getOperatorOptions,
    getPropertyById,
    getValueOptions,
} from '@app/tests/builder/steps/definition/builder/property/utils';
import { PropertyPredicate } from '@app/tests/builder/steps/definition/builder/types';

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
