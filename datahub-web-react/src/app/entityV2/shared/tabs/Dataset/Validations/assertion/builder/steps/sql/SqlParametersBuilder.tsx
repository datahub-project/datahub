import { Form, InputNumber, Select } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { SqlParametersRangeBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/SqlParametersRangeBuilder';
import { SQL_CHANGE_TYPE_OPTIONS } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/utils';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';

import { AssertionStdOperator, AssertionStdParameterType, AssertionValueChangeType, SqlAssertionType } from '@types';

const StyledSelect = styled(Select)`
    width: 120px;
`;

const StyledFormItem = styled(Form.Item)`
    flex: 1;
    margin: 0;
`;

const StyledNumberInput = styled(InputNumber)`
    width: 100%;
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

export const SqlParametersBuilder = ({ value, onChange, disabled }: Props) => {
    const isMetricChange = value.assertion?.sqlAssertion?.type === SqlAssertionType.MetricChange;
    const changeType = value.assertion?.sqlAssertion?.changeType;
    const isPercentage = changeType === AssertionValueChangeType.Percentage;

    const updateMetricChange = (newChangeType: AssertionValueChangeType) => {
        onChange({
            ...value,
            assertion: {
                ...value.assertion,
                sqlAssertion: {
                    ...value.assertion?.sqlAssertion,
                    changeType: newChangeType,
                },
            },
        });
    };

    const updateValue = (newValue: string | number | null) => {
        onChange({
            ...value,
            assertion: {
                ...value.assertion,
                sqlAssertion: {
                    ...value.assertion?.sqlAssertion,
                    parameters: {
                        ...value.assertion?.sqlAssertion?.parameters,
                        value: {
                            type: AssertionStdParameterType.Number,
                            value: (newValue || 0).toString(),
                        },
                    },
                },
            },
        });
    };

    if (value.assertion?.sqlAssertion?.operator === AssertionStdOperator.Between) {
        return (
            <SqlParametersRangeBuilder
                value={value}
                onChange={onChange}
                updateMetricChange={updateMetricChange}
                disabled={disabled}
            />
        );
    }

    const selectedValue = value.assertion?.sqlAssertion?.parameters?.value?.value;

    return (
        <StyledFormItem
            initialValue={selectedValue}
            name="sqlParameters.value"
            rules={[{ required: true, message: 'Required' }]}
        >
            <StyledNumberInput
                value={selectedValue}
                onChange={updateValue}
                disabled={disabled}
                addonAfter={isPercentage ? '%' : undefined}
                addonBefore={
                    isMetricChange ? (
                        <StyledSelect
                            value={changeType}
                            onChange={(newChangeType) => updateMetricChange(newChangeType as AssertionValueChangeType)}
                            options={SQL_CHANGE_TYPE_OPTIONS}
                            disabled={disabled}
                        />
                    ) : undefined
                }
            />
        </StyledFormItem>
    );
};
