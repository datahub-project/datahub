import React from 'react';
import styled from 'styled-components';
import { Input } from 'antd';
import { ValueSelect } from './select/ValueSelect';
import { PropertyPredicate } from '../types';
import { ValueInputType } from './types/values';
import { isUnaryOperator } from './types/operators';

const Container = styled.div`
    display: flex;
    align-items: center;
`;

const PropertyInput = styled(Input)`
    width: 200px;
    margin-right: 12px;
`;

const OperatorInput = styled(Input)`
    width: 200px;
    margin-right: 12px;
`;

type Props = {
    selectedPredicate?: PropertyPredicate;
    onChangeProperty: (newPropertyId: string) => void;
    onChangeOperator: (newOperatorId: string) => void;
    onChangeValues: (newValues: string[]) => void;
};

/**
 * This component allows you to construct a single Custom Property Predicate.
 *
 * A "custom" property predicate is one where the property is not well-supported / typed,
 * or does not appear in the fixed set of properties we make available to the end user.
 */
export const CustomPropertyPredicateBuilder = ({
    selectedPredicate,
    onChangeProperty,
    onChangeOperator,
    onChangeValues,
}: Props) => {
    const selectedProperty = selectedPredicate?.property;
    const selectedOperator = selectedPredicate?.operator;
    const selectedValues = selectedPredicate?.values;
    return (
        <Container>
            <PropertyInput
                value={selectedProperty}
                placeholder="Type a property..."
                onChange={(e) => onChangeProperty(e.target.value)}
            />
            {selectedProperty && (
                <OperatorInput
                    value={selectedOperator}
                    placeholder="Type an operator..."
                    onChange={(e) => onChangeOperator(e.target.value)}
                />
            )}
            {selectedOperator && !isUnaryOperator(selectedOperator) && (
                <ValueSelect
                    selectedValues={selectedValues}
                    options={{ inputType: ValueInputType.TEXT, options: undefined }}
                    onChangeValues={onChangeValues}
                />
            )}
        </Container>
    );
};
