import React from 'react';

import Condition from '@app/sharedV2/queryBuilder/Condition';
import { useOperandKeys } from '@app/sharedV2/queryBuilder/Operands.hooks';
/* eslint-disable import/no-cycle */
import QueryBuilder from '@app/sharedV2/queryBuilder/QueryBuilder';
import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { isLogicalPredicate } from '@app/sharedV2/queryBuilder/builder/utils';

interface Props {
    operands: (LogicalPredicate | PropertyPredicate)[];
    onChangeOperands: (operands) => void;
    onDeletePredicate: (index) => void;
    properties: Property[];
    depth: number;
    hideAddGroup?: boolean;
}

const Operands = ({ operands, onChangeOperands, onDeletePredicate, properties, depth, hideAddGroup }: Props) => {
    const { keys, markRemoval } = useOperandKeys(operands.length);

    const onDeleteCondition = (index: number) => {
        markRemoval(index);
        onDeletePredicate(index);
    };

    const onUpdatePredicate = (newPredicate, index) => {
        const newOperands = [...operands];
        if (newPredicate === undefined) {
            markRemoval(index);
            newOperands.splice(index, 1);
        } else {
            newOperands[index] = newPredicate;
        }
        onChangeOperands(newOperands);
    };

    return (
        <>
            {/* Display all the conditions first, then all the groups in a group preserving the index */}

            {operands.map((operand, index) =>
                isLogicalPredicate(operand) ? null : (
                    <Condition
                        key={keys[index]}
                        selectedPredicate={operand}
                        onChangePredicate={(pred) => onUpdatePredicate(pred, index)}
                        onDeletePredicate={() => onDeleteCondition(index)}
                        properties={properties}
                        index={index}
                        depth={depth + 1}
                    />
                ),
            )}
            {operands.map((operand, index) =>
                isLogicalPredicate(operand) ? (
                    <QueryBuilder
                        key={keys[index]}
                        selectedPredicate={operand as LogicalPredicate}
                        onChangePredicate={(pred) => onUpdatePredicate(pred, index)}
                        properties={properties}
                        depth={depth + 1}
                        index={index}
                        hideAddGroup={hideAddGroup}
                    />
                ) : null,
            )}
        </>
    );
};

export default Operands;
