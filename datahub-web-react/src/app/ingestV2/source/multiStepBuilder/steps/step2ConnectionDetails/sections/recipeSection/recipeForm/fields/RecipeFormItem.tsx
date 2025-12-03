import React from 'react';

import { RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

import { CustomLabelFormItem, CustomLabelFormItemProps } from '../components/CustomFormItem';
import { HelperText } from './HelperText';
import styled from 'styled-components';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

interface Props extends CustomLabelFormItemProps {
    recipeField?: RecipeField;
    showTooltip?: boolean;
    showHelperText?: boolean;
}

export function RecipeFormItem({
    children,
    recipeField,
    showHelperText,
    showTooltip,
    tooltip,
    ...props
}: React.PropsWithChildren<Props>) {
    return (
        <Wrapper>
            <CustomLabelFormItem
                required={recipeField?.required}
                label={recipeField?.label}
                name={recipeField?.name}
                tooltip={showTooltip ? (recipeField?.tooltip ?? tooltip) : undefined}
                rules={recipeField?.rules || undefined}
                {...props}
            >
                {children}
            </CustomLabelFormItem>
            {showHelperText && recipeField?.helper && <HelperText text={recipeField.helper} />}
        </Wrapper>
    );
}
