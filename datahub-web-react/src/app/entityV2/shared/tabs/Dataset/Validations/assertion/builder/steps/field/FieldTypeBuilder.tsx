import { Tooltip } from '@components';
import { Form, Select } from 'antd';
import Typography from 'antd/lib/typography';
import React from 'react';
import styled from 'styled-components';

import { useConnectionForEntityExists } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import {
    getDatasetProfileDisabledMessage,
    getDefaultDatasetFieldAssertionParametersState,
    getDefaultDatasetFieldAssertionState,
    getFieldTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';

import { FieldAssertionType } from '@types';

const Section = styled.div`
    margin: 16px 0;
`;

const StyledSelect = styled(Select)`
    width: 240px;
`;

const SelectOptionContent = styled.div<{ disabled: boolean }>`
    opacity: ${(props) => (props.disabled ? 0.5 : 1)};
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
    disabled?: boolean;
};

export const FieldTypeBuilder = ({ value, onChange, disabled }: Props) => {
    const form = Form.useFormInstance();
    const options = getFieldTypeOptions();
    const connectionForEntityExists = useConnectionForEntityExists(value.entityUrn as string);
    const defaultAssertionState = getDefaultDatasetFieldAssertionState(connectionForEntityExists);
    const defaultParameterState = getDefaultDatasetFieldAssertionParametersState(connectionForEntityExists);

    const updateFieldType = (newFieldType: FieldAssertionType) => {
        onChange({
            ...value,
            assertion: {
                ...value.assertion,
                fieldAssertion: {
                    ...defaultAssertionState,
                    type: newFieldType,
                },
            },
            parameters: defaultParameterState,
        });
        form.resetFields();
    };

    return (
        <Section>
            <Typography.Title level={5}>Type</Typography.Title>
            <Typography.Paragraph type="secondary">Select the column assertion type</Typography.Paragraph>
            <StyledSelect
                value={value.assertion?.fieldAssertion?.type}
                onChange={(newFieldType) => updateFieldType(newFieldType as FieldAssertionType)}
                disabled={disabled}
            >
                {options.map((option) => {
                    const disabledMessage = getDatasetProfileDisabledMessage(
                        value.platformUrn as string,
                        option.requiresConnection,
                        connectionForEntityExists,
                    );
                    return (
                        <Select.Option key={option.value} value={option.value} disabled={!!disabledMessage}>
                            <Tooltip placement="right" title={disabledMessage || undefined}>
                                <SelectOptionContent disabled={!!disabledMessage}>
                                    <Typography.Text>{option.label}</Typography.Text>
                                    <OptionDescription type="secondary">{option.description}</OptionDescription>
                                </SelectOptionContent>
                            </Tooltip>
                        </Select.Option>
                    );
                })}
            </StyledSelect>
        </Section>
    );
};
