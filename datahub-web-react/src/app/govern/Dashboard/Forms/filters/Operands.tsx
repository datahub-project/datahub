import { Property } from '@src/app/automations/fields/ConditionSelector/properties';
import { LogicalPredicate, PropertyPredicate } from '@src/app/tests/builder/steps/definition/builder/types';
import { isLogicalPredicate } from '@src/app/tests/builder/steps/definition/builder/utils';
import React from 'react';
import Condition from './Condition';
/* eslint-disable import/no-cycle */
import QueryBuilder from './QueryBuilder';

interface Props {
    operands: (LogicalPredicate | PropertyPredicate)[];
    onChangeOperands: (operands) => void;
    onDeletePredicate: (index) => void;
    properties: Property[];
    depth: number;
}

const Operands = ({ operands, onChangeOperands, onDeletePredicate, properties, depth }: Props) => {
    const onUpdatePredicate = (newPredicate, index) => {
        const newOperands = [...operands];
        if (newPredicate === undefined) {
            newOperands.splice(index, 1);
        } else {
            newOperands[index] = newPredicate;
        }
        onChangeOperands(newOperands);
    };

    return (
        <>
            {/* Display all the conditions first, then all the groups in a group preserving the index */}

            {operands.map((operand, index) => (
                <>
                    {!isLogicalPredicate(operand) && (
                        <Condition
                            selectedPredicate={operand}
                            onChangePredicate={(pred) => onUpdatePredicate(pred, index)}
                            onDeletePredicate={() => onDeletePredicate(index)}
                            properties={properties}
                            index={index}
                            depth={depth + 1}
                        />
                    )}
                </>
            ))}
            {operands.map((operand, index) => (
                <>
                    {isLogicalPredicate(operand) && (
                        <QueryBuilder
                            selectedPredicate={operand as LogicalPredicate}
                            onChangePredicate={(pred) => onUpdatePredicate(pred, index)}
                            properties={properties}
                            depth={depth + 1}
                            index={index}
                        />
                    )}
                </>
            ))}
        </>
    );
};

export default Operands;
