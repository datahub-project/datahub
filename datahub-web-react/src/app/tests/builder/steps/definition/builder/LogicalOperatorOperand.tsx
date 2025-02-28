import { DeleteOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
/* eslint-disable import/no-cycle */
import { ANTD_GRAY } from '../../../../../entity/shared/constants';
import { LogicalPredicateBuilder } from './LogicalPredicateBuilder';
import { PropertyPredicateBuilder } from './property/PropertyPredicateBuilder';
import { Property } from './property/types/properties';
import { LogicalPredicate, PropertyPredicate } from './types';
import { isLogicalPredicate } from './utils';

const PredicateContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
    margin-top: 12px;
    margin-bottom: 12px;
    margin-right: 8px;
`;

const DeleteButton = styled(Button)`
    margin: 0px;
    padding: 0px;
    margin-left: 8px;
`;

const DeleteIcon = styled(DeleteOutlined)`
    && {
        font-size: 14px;
        color: ${ANTD_GRAY[7]};
    }
`;

type Props = {
    operand: LogicalPredicate | PropertyPredicate;
    onChange: (newOperand: LogicalPredicate | PropertyPredicate) => void;
    onDelete: () => void;
    properties: Property[];
    options?: any;
};

export const LogicalOperatorOperand = ({ operand, onChange, onDelete, properties, options }: Props) => {
    /**
     * Whether this operand is a logical operand (and / or / not), or a property
     * operand (prop, op, vals)
     */
    const isLogPred = isLogicalPredicate(operand);

    const predicateBuilder = isLogPred ? (
        <LogicalPredicateBuilder
            selectedPredicate={operand as LogicalPredicate}
            onChangePredicate={onChange}
            properties={properties}
            options={options}
        />
    ) : (
        <PropertyPredicateBuilder
            selectedPredicate={operand as PropertyPredicate}
            onChangePredicate={onChange}
            properties={properties}
        />
    );

    return (
        <PredicateContainer>
            {predicateBuilder}
            <DeleteButton type="text" onClick={onDelete}>
                <DeleteIcon />
            </DeleteButton>
        </PredicateContainer>
    );
};
