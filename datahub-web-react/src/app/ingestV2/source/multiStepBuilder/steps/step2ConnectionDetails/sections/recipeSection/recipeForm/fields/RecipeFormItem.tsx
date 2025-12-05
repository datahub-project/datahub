import React, { useMemo } from 'react';
import styled from 'styled-components';

import { RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';
import {
    CustomLabelFormItem,
    CustomLabelFormItemProps,
} from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/CustomFormItem';
import { HelperText } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/shared/HelperText';

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
    const rules = useMemo(() => {
        if (recipeField?.rules) return recipeField.rules;
        if (recipeField?.required) return [{ required: true, message: `${recipeField.label} is required` }];
        return undefined;
    }, [recipeField]);

    return (
        <Wrapper>
            <CustomLabelFormItem
                required={recipeField?.required}
                label={recipeField?.label}
                name={recipeField?.name}
                tooltip={showTooltip ? (recipeField?.tooltip ?? tooltip) : undefined}
                rules={rules}
                {...props}
            >
                {children}
            </CustomLabelFormItem>
            {showHelperText && recipeField?.helper && <HelperText text={recipeField.helper} />}
        </Wrapper>
    );
}
