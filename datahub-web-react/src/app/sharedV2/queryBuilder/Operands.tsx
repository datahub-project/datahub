import React from 'react';

import Condition from '@app/sharedV2/queryBuilder/Condition';
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
                            hideAddGroup={hideAddGroup}
                        />
                    )}
                </>
            ))}
        </>
    );
};

export default Operands;
