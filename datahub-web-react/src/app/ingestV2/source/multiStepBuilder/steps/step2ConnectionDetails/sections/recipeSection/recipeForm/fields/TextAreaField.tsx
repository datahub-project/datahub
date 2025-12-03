import { TextArea } from '@components';

import { RecipeFormItem } from './RecipeFormItem';
import { CommonFieldProps } from './types';

export function TextAreaField({ field }: CommonFieldProps) {
    return (
        <RecipeFormItem
            recipeField={field}
            valuePropName="value"
            getValueFromEvent={(e) => (e.target.value === '' ? null : e.target.value)}
            showHelperText
        >
            <TextArea required={field.required} placeholder={field.placeholder} />
        </RecipeFormItem>
    );
}
