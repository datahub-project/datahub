import { Text, Tooltip } from '@components';
import { Select } from 'antd';
import React from 'react';

import { Operator } from '@app/sharedV2/queryBuilder/builder/property/types/operators';
import { StyledSelect } from '@app/sharedV2/queryBuilder/styledComponents';

interface Props {
    selectedOperator?: string;
    operators?: Operator[];
    onChangeOperator: (newOperatorId) => void;
}

const OperatorSelect = ({ selectedOperator, operators, onChangeOperator }: Props) => {
    return (
        <StyledSelect
            defaultActiveFirstOption={false}
            placeholder="Select an operator..."
            onSelect={(val) => onChangeOperator(val)}
            value={selectedOperator?.toLowerCase()}
            disabled={!operators}
            data-testid="condition-operator-select"
        >
            {operators?.map((operator) => {
                return (
                    <Select.Option
                        value={operator.id.toLowerCase()}
                        key={operator.id.toLowerCase()}
                        data-testid={`condition-operator-select-option-${operator.id.toLowerCase()}`}
                    >
                        <Tooltip title={operator.description} placement="right" showArrow={false}>
                            <Text color="gray" type="span">
                                {operator.displayName}
                            </Text>
                        </Tooltip>
                    </Select.Option>
                );
            })}
        </StyledSelect>
    );
};

export default OperatorSelect;
