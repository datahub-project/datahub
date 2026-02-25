import React from 'react';

import { AntdFormCompatibleCheckbox } from '@app/ingestV2/source/multiStepBuilder/components/AntdCompatibleCheckbox';
import { RecipeFormItem } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/RecipeFormItem';
import { CommonFieldProps } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/types';

export function CheckboxField({ field }: CommonFieldProps) {
    return (
        <RecipeFormItem
            recipeField={field}
            style={{ flexDirection: 'row', alignItems: 'center' }}
            valuePropName="checked"
        >
            <AntdFormCompatibleCheckbox helper={field.helper ?? field.tooltip} disabled={field.disabled} />
        </RecipeFormItem>
    );
}
