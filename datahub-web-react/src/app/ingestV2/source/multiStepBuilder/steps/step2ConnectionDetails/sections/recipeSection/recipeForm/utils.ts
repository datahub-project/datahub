import { get } from 'lodash';
import YAML from 'yamljs';

import { RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

export function getValuesFromRecipe(displayRecipe: string, allFields: RecipeField[]) {
    const initialValues = {};
    const recipeObj = YAML.parse(displayRecipe);

    if (recipeObj) {
        allFields.forEach((field) => {
            if (field.getValueFromRecipeOverride) {
                initialValues[field.name] = field.getValueFromRecipeOverride(recipeObj);
            } else {
                initialValues[field.name] = get(recipeObj, field.fieldPath);
            }
        });
    }

    return initialValues;
}
