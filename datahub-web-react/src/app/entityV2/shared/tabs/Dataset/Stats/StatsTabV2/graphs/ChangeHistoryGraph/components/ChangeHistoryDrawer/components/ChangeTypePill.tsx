/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo } from 'react';

import { Pill } from '@src/alchemy-components';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';
import { Operation, OperationType } from '@src/types.generated';

type ChangeTypePillProps = {
    operation: Operation;
};

export default function ChangeTypePill({ operation }: ChangeTypePillProps) {
    const changeTypeName = useMemo(() => {
        if (operation.customOperationType) return operation.customOperationType;
        return capitalizeFirstLetter(operation.operationType) as string;
    }, [operation]);

    const colorScheme = useMemo(() => {
        switch (operation.operationType) {
            case OperationType.Delete:
            case OperationType.Drop:
                return 'red';
            default:
                return 'violet';
        }
    }, [operation.operationType]);

    return <Pill size="sm" label={changeTypeName} color={colorScheme} clickable={false} />;
}
