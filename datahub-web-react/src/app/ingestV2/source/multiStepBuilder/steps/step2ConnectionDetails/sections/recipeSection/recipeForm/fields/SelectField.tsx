import { SimpleSelect } from '@components';
import { Form } from 'antd';
import React from 'react';

import { RecipeFormItem } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/RecipeFormItem';
import { CommonFieldProps } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/types';

export function SelectField({ field, updateFormValue }: CommonFieldProps) {
    const form = Form.useFormInstance();
    const value = Form.useWatch([field.name], form);
    const { isMultiSelect } = field;

    const selectedValues = (() => {
        if (isMultiSelect) return Array.isArray(value) ? value : [];
        return value ? [value] : [];
    })();

    const handleUpdate = (values?: string[]) => {
        const nextValue = isMultiSelect ? (values ?? []) : values?.[0];
        form.setFieldsValue({ [field.name]: nextValue });
        updateFormValue(field.name, nextValue);
    };

    return (
        <RecipeFormItem recipeField={field} showHelperText>
            <SimpleSelect
                values={selectedValues}
                onUpdate={handleUpdate}
                placeholder={field.placeholder}
                options={field.options ?? []}
                showClear={!field.required}
                isMultiSelect={isMultiSelect}
                width="full"
            />
        </RecipeFormItem>
    );
}
