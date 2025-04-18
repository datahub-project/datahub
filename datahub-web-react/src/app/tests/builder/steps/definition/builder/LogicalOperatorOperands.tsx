import React from 'react';

import { LogicalOperatorOperand } from '@app/tests/builder/steps/definition/builder/LogicalOperatorOperand';
import { Property } from '@app/tests/builder/steps/definition/builder/property/types/properties';
/* eslint-disable import/no-cycle */
import { LogicalPredicate, PropertyPredicate } from '@app/tests/builder/steps/definition/builder/types';

type Props = {
    operands: (LogicalPredicate | PropertyPredicate)[];
    onChangeOperands: (operator) => void;
    properties: Property[];
    options?: any;
};

export const LogicalOperatorOperands = ({ operands, onChangeOperands, properties, options }: Props) => {
    const onUpdatePredicate = (newPredicate, index) => {
        const newOperands = [...operands];
        newOperands[index] = newPredicate;
        onChangeOperands(newOperands);
    };

    const onDeletePredicate = (index) => {
        const newOperands = [...operands];
        newOperands.splice(index, 1);
        onChangeOperands(newOperands);
    };

    return (
        <>
            {operands.map((operand, index) => (
                <LogicalOperatorOperand
                    operand={operand}
                    properties={properties}
                    onChange={(pred) => onUpdatePredicate(pred, index)}
                    onDelete={() => onDeletePredicate(index)}
                    options={options}
                />
            ))}
        </>
    );
};
