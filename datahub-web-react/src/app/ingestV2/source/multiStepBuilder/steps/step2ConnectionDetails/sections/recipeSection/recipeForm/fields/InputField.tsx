import { Input } from '@components';

import { RecipeFormItem } from './RecipeFormItem';
import { CommonFieldProps } from './types';

export function InputField({ field }: CommonFieldProps) {
    return (
        <RecipeFormItem
            recipeField={field}
            valuePropName={'value'}
            getValueFromEvent={(e) => (e.target.value === '' ? null : e.target.value)}
            showHelperText
        >
            <Input required={field.required} isRequired={field.required} placeholder={field.placeholder} />
        </RecipeFormItem>
    );
}
