import React from 'react';
import Typography from 'antd/lib/typography';
import styled from 'styled-components';
import { Select } from 'antd';
import { AssertionMonitorBuilderState } from '../../types';
import { getFieldTypeOptions } from './utils';
import { FieldAssertionType } from '../../../../../../../../../../types.generated';

const Section = styled.div`
    margin: 16px 0;
`;

const StyledSelect = styled(Select)`
    width: 240px;
`;

const SelectOptionContent = styled.div<{ disabled: boolean }>`
    opacity: ${(props) => (props.disabled ? 0.5 : 1)};
`;

const OptionLabel = styled(Typography.Text)`
    display: block;
    margin-bottom: 8px;
`;

const OptionDescription = styled(Typography.Paragraph)`
    && {
        margin: 0;
        padding: 0;
        overflow-wrap: break-word;
        white-space: normal;
    }
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
};

export const FieldTypeBuilder = ({ value, onChange }: Props) => {
    const options = getFieldTypeOptions();
    const updateFieldType = (newFieldType: FieldAssertionType) => {
        onChange({
            ...value,
            assertion: {
                ...value.assertion,
                fieldAssertion: {
                    ...value.assertion?.fieldAssertion,
                    type: newFieldType,
                },
            },
        });
    };

    return (
        <Section>
            <Typography.Title level={5}>Type</Typography.Title>
            <Typography.Paragraph type="secondary">Select the column assertion type</Typography.Paragraph>
            <StyledSelect
                value={value.assertion?.fieldAssertion?.type}
                onChange={(newFieldType) => updateFieldType(newFieldType as FieldAssertionType)}
            >
                {options.map((option) => (
                    <Select.Option key={option.value} value={option.value} disabled={option.disabled}>
                        <SelectOptionContent disabled={option.disabled}>
                            <OptionLabel>{option.label}</OptionLabel>
                            <OptionDescription type="secondary">{option.description}</OptionDescription>
                        </SelectOptionContent>
                    </Select.Option>
                ))}
            </StyledSelect>
        </Section>
    );
};
