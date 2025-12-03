import { Form, FormItemProps } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { FieldLabel } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/FieldLabel';

export const StyledFormItem = styled(Form.Item)<{
    $alignLeft?: boolean;
    $removeMargin?: boolean;
    $isSecretField?: boolean;
}>`
    margin-bottom: 0;

    .ant-form-item-label > label {
        &:before {
            content: '' !important; // Remove default required mark
            margin-right: 0 !important;
        }

        padding: 0;
    }
`;

export interface CustomLabelFormItemProps extends Omit<FormItemProps, 'label'> {
    label?: string;
    tooltip?: React.ReactNode;
    labelHelper?: React.ReactNode;
}

export const CustomLabelFormItem: React.FC<CustomLabelFormItemProps> = ({
    label,
    tooltip,
    required,
    labelHelper,
    ...formItemProps
}) => {
    return (
        <StyledFormItem
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
