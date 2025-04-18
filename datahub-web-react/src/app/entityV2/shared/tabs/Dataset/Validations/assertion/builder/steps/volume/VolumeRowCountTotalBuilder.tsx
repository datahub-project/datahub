import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { VolumeNumberInput } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/VolumeNumberInput';
import { VolumeAssertionBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { getPropertyFromVolumeType } from '@app/entityV2/shared/tabs/Dataset/Validations/utils';

import { AssertionStdOperator, AssertionStdParameterType } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

type Props = {
    volumeInfo: VolumeAssertionBuilderState;
    value: VolumeAssertionBuilderState['parameters'];
    onChange: (newParams: VolumeAssertionBuilderState['parameters']) => void;
    disabled?: boolean;
};

export const VolumeRowCountTotalBuilder = ({ volumeInfo, value, onChange, disabled }: Props) => {
    const selectedType = volumeInfo.type;
    const propertyName = selectedType ? getPropertyFromVolumeType(selectedType) : '';
    const operator = volumeInfo[propertyName]?.operator;
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
                    value={value?.value?.value ? parseInt(value.value.value, 10) : undefined}
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
                        value={value?.minValue?.value ? parseInt(value?.minValue?.value, 10) : undefined}
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
                    <Typography.Text strong>Or at most</Typography.Text>
                    <VolumeNumberInput
                        name="parameters.maxValue"
                        placeholder="Max"
                        value={value?.maxValue?.value ? parseInt(value.maxValue.value, 10) : undefined}
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
