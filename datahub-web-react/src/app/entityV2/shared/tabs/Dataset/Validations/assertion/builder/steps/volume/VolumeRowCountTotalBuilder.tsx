import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import {
    AssertionStdOperator,
    AssertionStdParameterType,
    AssertionStdParameters,
    VolumeAssertionInfo,
} from '../../../../../../../../../../types.generated';
import { getPropertyFromVolumeType } from './utils';
import { VolumeNumberInput } from './VolumeNumberInput';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

type Props = {
    volumeInfo: VolumeAssertionInfo;
    value: AssertionStdParameters;
    onChange: (newParams: AssertionStdParameters) => void;
    disabled?: boolean;
};

export const VolumeRowCountTotalBuilder = ({ volumeInfo, value, onChange, disabled }: Props) => {
    const selectedType = volumeInfo.type;
    const propertyName = getPropertyFromVolumeType(selectedType);
    const operator = volumeInfo[propertyName]?.operator as AssertionStdOperator;
    const handleValueChange = (newValue: number) => {
        onChange({
            ...value,
            value: {
                type: AssertionStdParameterType.Number,
                value: newValue?.toString(),
            },
        });
    };
    const handleMinValueChange = (newValue: number) => {
        onChange({
            ...value,
            minValue: {
                type: AssertionStdParameterType.Number,
                value: newValue?.toString(),
            },
        });
    };
    const handleMaxValueChange = (newValue: number) => {
        onChange({
            ...value,
            maxValue: {
                type: AssertionStdParameterType.Number,
                value: newValue?.toString(),
            },
        });
    };

    switch (operator) {
        case AssertionStdOperator.GreaterThanOrEqualTo:
        case AssertionStdOperator.LessThanOrEqualTo:
            return (
                <VolumeNumberInput
                    name="parameters.value"
                    placeholder="Number"
                    value={value.value?.value ? parseInt(value.value.value, 10) : undefined}
                    onChange={(newValue) => handleValueChange(newValue as number)}
                    disabled={disabled}
                />
            );
        case AssertionStdOperator.Between:
            return (
                <Container>
                    <VolumeNumberInput
                        name="parameters.minValue"
                        placeholder="Min"
                        value={value.minValue?.value ? parseInt(value.minValue.value, 10) : undefined}
                        onChange={(newValue) => handleMinValueChange(newValue as number)}
                        disabled={disabled}
                        customRules={[
                            ({ getFieldValue }) => ({
                                validator(_, fieldValue) {
                                    if (fieldValue >= getFieldValue('parameters.maxValue')) {
                                        return Promise.reject(new Error('Must be less than maximum'));
                                    }
                                    return Promise.resolve();
                                },
                            }),
                        ]}
                    />
                    <Typography.Text strong>Or more than</Typography.Text>
                    <VolumeNumberInput
                        name="parameters.maxValue"
                        placeholder="Max"
                        value={value.maxValue?.value ? parseInt(value.maxValue.value, 10) : undefined}
                        onChange={(newValue) => handleMaxValueChange(newValue as number)}
                        disabled={disabled}
                        customRules={[
                            ({ getFieldValue }) => ({
                                validator(_, fieldValue) {
                                    if (fieldValue <= getFieldValue('parameters.minValue')) {
                                        return Promise.reject(new Error('Must be greater than minimum'));
                                    }
                                    return Promise.resolve();
                                },
                            }),
                        ]}
                    />
                </Container>
            );
        default:
            return null;
    }
};
