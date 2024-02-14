import React, { useEffect } from 'react';
import Typography from 'antd/lib/typography';
import styled from 'styled-components';
import { Form, Select } from 'antd';
import useFormInstance from 'antd/lib/form/hooks/useFormInstance';
import { AssertionMonitorBuilderState } from '../../types';
import { FieldMetricType } from '../../../../../../../../../../types.generated';
import { getFieldMetricTypeOptions } from './utils';
import { FieldMetricParameterBuilder } from './FieldMetricParameterBuilder';

const Section = styled.div`
    margin-top: 16px;
`;

const StyledFormItem = styled(Form.Item)`
    width: 240px;
    margin: 0;
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

export const FieldMetricBuilder = ({ value, onChange, disabled }: Props) => {
    const form = useFormInstance();
    const fieldType = value.assertion?.fieldAssertion?.fieldMetricAssertion?.field?.type;
    const metricType = value.assertion?.fieldAssertion?.fieldMetricAssertion?.metric;
    const sourceType = value.parameters?.datasetFieldParameters?.sourceType;
    const options = getFieldMetricTypeOptions(fieldType, sourceType);

    const updateMetricType = (newMetricType: FieldMetricType) => {
        onChange({
            ...value,
            assertion: {
                ...value.assertion,
                fieldAssertion: {
                    ...value.assertion?.fieldAssertion,
                    fieldMetricAssertion: {
                        ...value.assertion?.fieldAssertion?.fieldMetricAssertion,
                        metric: newMetricType,
                        operator: undefined,
                        parameters: undefined,
                    },
                },
            },
        });
    };

    useEffect(() => {
        form.setFieldValue('fieldMetricType', metricType);
    }, [form, metricType]);

    return (
        <Section>
            <Typography.Title level={5}>Metric</Typography.Title>
            <Typography.Paragraph type="secondary">Select a column metric to evaluate</Typography.Paragraph>
            <StyledFormItem
                name="fieldMetricType"
                rules={[
                    {
                        required: true,
                        message: 'Required',
                    },
                ]}
            >
                <Select
                    placeholder="Select a metric"
                    onChange={(newMetricType) => updateMetricType(newMetricType)}
                    options={options}
                    disabled={disabled}
                />
            </StyledFormItem>
            {metricType && <FieldMetricParameterBuilder value={value} onChange={onChange} disabled={disabled} />}
        </Section>
    );
};
