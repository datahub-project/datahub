import React from 'react';
import { Form, InputNumber, Typography } from 'antd';
import styled from 'styled-components';
import { AssertionMonitorBuilderState } from '../../types';
import { FieldAssertionType, FieldValuesFailThresholdType } from '../../../../../../../../../../types.generated';

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
};

const Section = styled.div`
    margin-top: 24px;
`;

const StyledFormItem = styled(Form.Item)`
    width: 240px;
    margin: 0;
`;

export const FieldErrorThresholdBuilder = ({ value, onChange }: Props) => {
    const updateErrorThreshold = (newValue: number | null) => {
        onChange({
            ...value,
            assertion: {
                ...value.assertion,
                fieldAssertion: {
                    ...value.assertion?.fieldAssertion,
                    fieldValuesAssertion: {
                        ...value.assertion?.fieldAssertion?.fieldValuesAssertion,
                        failThreshold: {
                            type: FieldValuesFailThresholdType.Count,
                            value: newValue,
                        },
                    },
                },
            },
        });
    };

    return value.assertion?.fieldAssertion?.type === FieldAssertionType.FieldValues ? (
        <Section>
            <Typography.Title level={5}>Invalid Values Threshold </Typography.Title>
            <Typography.Paragraph type="secondary">
                The maximum number of column values (rows) that are allowed to fail the condition before the assertion
                fails. By default this is 0, meaning the assertion will fail if any rows have an invalid column value.
            </Typography.Paragraph>
            <StyledFormItem
                initialValue={value.assertion?.fieldAssertion?.fieldValuesAssertion?.failThreshold?.value}
                name="errorThreshold"
                rules={[{ required: true, message: 'Please enter a threshold' }]}
            >
                <InputNumber
                    value={value.assertion?.fieldAssertion?.fieldValuesAssertion?.failThreshold?.value}
                    onChange={(newValue) => updateErrorThreshold(newValue)}
                />
            </StyledFormItem>
        </Section>
    ) : null;
};
