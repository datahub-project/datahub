import { colors } from '@components';
import { Form, FormItemProps } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { FieldLabel } from '@app/sharedV2/forms/FieldLabel';

const StyledFormItem = styled(Form.Item)<{ $showError: boolean }>`
    margin-bottom: 0;

    .ant-form-item-label > label {
        &:before {
            content: '' !important; // Remove default required mark
            margin-right: 0 !important;
        }

        padding: 0;
    }

    .ant-form-item-explain-error {
        color: ${colors.red[500]}; // Color of error message
        display: none;
        ${({ $showError }) => !$showError && 'display: none;'}
    }

    .ant-form-item-control {
        margin-left: 1px; // Inputs from components library have 1px outline when focused so it prevents outline cutting off
    }
`;

export interface CustomLabelFormItemProps extends Omit<FormItemProps, 'label'> {
    label?: string;
    tooltip?: React.ReactNode;
    labelHelper?: React.ReactNode;
    showError?: boolean;
}

export const CustomLabelFormItem: React.FC<CustomLabelFormItemProps> = ({
    label,
    tooltip,
    required,
    labelHelper,
    showError = true,
    ...formItemProps
}) => {
    return (
        <StyledFormItem
            $showError={showError}
            label={
                label ? (
                    <FieldLabel label={label} required={required} tooltip={tooltip} labelHelper={labelHelper} />
                ) : undefined
            }
            {...formItemProps}
            required={required}
        />
    );
};
