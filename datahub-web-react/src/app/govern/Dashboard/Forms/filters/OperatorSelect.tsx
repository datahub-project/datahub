import { Text, Tooltip } from '@components';
import { Operator } from '@src/app/tests/builder/steps/definition/builder/property/types/operators';
import { Select } from 'antd';
import React from 'react';
import { StyledSelect } from './styledComponents';

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
        >
            {operators?.map((operator) => {
                return (
                    <Select.Option value={operator.id.toLowerCase()} key={operator.id.toLowerCase()}>
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
