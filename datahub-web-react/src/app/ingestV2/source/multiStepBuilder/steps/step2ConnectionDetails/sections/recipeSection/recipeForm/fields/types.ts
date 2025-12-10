import { RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

export interface CommonFieldProps {
    field: RecipeField;
    updateFormValue: (field, value) => void;
}
