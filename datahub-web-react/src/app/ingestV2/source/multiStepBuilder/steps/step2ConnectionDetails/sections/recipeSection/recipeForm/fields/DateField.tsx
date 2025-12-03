import { DatePicker } from 'antd';

import { RecipeFormItem } from './RecipeFormItem';
import { CommonFieldProps } from './types';

export function DateField({ field }: CommonFieldProps) {
    return (
        <RecipeFormItem recipeField={field} showTooltip>
            <DatePicker showTime />
        </RecipeFormItem>
    );
}
