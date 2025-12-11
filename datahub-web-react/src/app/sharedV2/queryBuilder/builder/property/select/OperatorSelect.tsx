/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from '@components';
import { Select, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { Operator } from '@app/sharedV2/queryBuilder/builder/property/types/operators';

const StyledSelect = styled(Select)`
    max-width: 160px;
    margin-right: 12px;
`;

type Props = {
    selectedOperator?: string;
    operators: Operator[];
    onChangeOperator: (newOperatorId: string) => void;
};

/**
 * A single node in a menu tree. This node can have children corresponding
 * to properties that should appear nested inside of it.
 */
export const OperatorSelect = ({ selectedOperator, operators, onChangeOperator }: Props) => {
    return (
        <StyledSelect
            defaultActiveFirstOption={false}
            placeholder="Select an operator..."
            onSelect={(newVal) => onChangeOperator(newVal as string)}
            value={selectedOperator?.toLowerCase()}
        >
            {operators?.map((operator) => {
                return (
                    <Select.Option value={operator.id.toLowerCase()} key={operator.id.toLowerCase()}>
                        <Tooltip title={operator.description} placement="right">
                            <Typography.Text>{operator.displayName}</Typography.Text>
                        </Tooltip>
                    </Select.Option>
                );
            })}
        </StyledSelect>
    );
};
