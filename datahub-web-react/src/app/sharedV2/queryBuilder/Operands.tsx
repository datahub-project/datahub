/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
