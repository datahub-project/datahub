import React from 'react';
import styled from 'styled-components';

/* eslint-disable import/no-cycle */
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { AddPredicateButton } from '@app/tests/builder/steps/definition/builder/AddPredicateButton';
import { LogicalOperatorDropdown } from '@app/tests/builder/steps/definition/builder/LogicalOperatorDropdown';
import { LogicalOperatorOperands } from '@app/tests/builder/steps/definition/builder/LogicalOperatorOperands';
import { Property } from '@app/tests/builder/steps/definition/builder/property/types/properties';
import {
    LogicalOperatorType,
    LogicalPredicate,
    PropertyPredicate,
} from '@app/tests/builder/steps/definition/builder/types';
import { isLogicalPredicate } from '@app/tests/builder/steps/definition/builder/utils';

/**
 * The maximum number of sub-predicates supported in a single
 * clause.
 */
const MAX_PREDICATES = 10;

const Container = styled.div`
    background-color: ${ANTD_GRAY[1]};
    border-radius: 4px;
    padding: 16px;
    border: 0.5px solid ${ANTD_GRAY[4]};
    display: flex;
    align-items: center;
    justify-content: left;
    overflow: auto;
    box-shadow: 0px 0px 6px 0px #e8e8e8;
`;

const OperandContainer = styled.div`
    margin-left: 8px;
`;

export const EMPTY_PROPERTY_PREDICATE = {
    property: undefined,
    operator: undefined,
    values: undefined,
};

type Options = {
    predicateDisplayName?: string;
    maxPredicates?: number;
};

type Props = {
    selectedPredicate: LogicalPredicate | PropertyPredicate;
    onChangePredicate: (newPredicate: LogicalPredicate) => void;
    properties: Property[]; // Set of properties eligible for use in nested property predicate builder.
    disabled?: boolean;
    options?: Options;
};

export const convertToLogicalPredicate = (predicate: LogicalPredicate | PropertyPredicate): LogicalPredicate => {
    // If we have a property predicate, simply convert to a basic logical predicate.
    if (!isLogicalPredicate(predicate)) {
        return {
            operator: LogicalOperatorType.AND,
            operands: [predicate],
        };
    }
    // Already is a logical predicate.
    return predicate as LogicalPredicate;
};

/**
 * This component can be used for building a Logical Predicate, which is an arbitrarily
 * nested series or and, or, not, and property predicate expressions.
 */
export const LogicalPredicateBuilder = ({
    selectedPredicate,
    onChangePredicate,
    properties,
    disabled = false,
    options = {
        maxPredicates: 10,
        predicateDisplayName: 'predicate',
    },
}: Props) => {
    const logicalPredicate = convertToLogicalPredicate(selectedPredicate);
    const { operator } = logicalPredicate;
    const operands = logicalPredicate?.operands || [];

    const onAddPropertyPredicate = () => {
        const newOperands = [...operands, EMPTY_PROPERTY_PREDICATE];
        onChangePredicate({ operator, operands: newOperands });
    };

    const onAddLogicalPredicate = (op: LogicalOperatorType) => {
        const newPredicate = {
            operator: op,
            operands: [],
        };
        const newOperands = [...operands, newPredicate];
        onChangePredicate({ operator, operands: newOperands });
    };

    const onChangeOperator = (newOperator) => {
        onChangePredicate({ operator: newOperator, operands });
    };

    const onChangeOperands = (ops) => {
        onChangePredicate({ operator, operands: ops });
    };

    const canAddOperand = operands.length < (options?.maxPredicates || MAX_PREDICATES);

    return (
        <Container>
            <LogicalOperatorDropdown
                operator={operator}
                onSelectOperator={onChangeOperator}
                predicateDisplayName={options.predicateDisplayName}
            />
            <OperandContainer>
                <LogicalOperatorOperands
                    operands={operands}
                    onChangeOperands={onChangeOperands}
                    properties={properties}
                    options={options}
                />
                {canAddOperand && (
                    <AddPredicateButton
                        disabled={disabled}
                        onAddPropertyPredicate={onAddPropertyPredicate}
                        onAddLogicalPredicate={onAddLogicalPredicate}
                        options={options}
                    />
                )}
            </OperandContainer>
        </Container>
    );
};
