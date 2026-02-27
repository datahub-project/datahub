import { SelectOption } from '@components';
import { v4 as uuidv4 } from 'uuid';

import { FieldType, FilterRecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';
import { getValuesFromRecipe } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/utils';
import { Filter } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/filtersSection/types';

export function getEmptyFilter(): Filter {
    return {
        key: uuidv4(),
        rule: undefined,
        subtype: undefined,
        value: '',
    };
}

export function getInitialFilters(fields: FilterRecipeField[], recipe: string) {
    const filters: Filter[] = [];

    let valuesFromRecipe = {};
    try {
        valuesFromRecipe = getValuesFromRecipe(recipe, fields);
    } catch {
        valuesFromRecipe = {};
    }

    fields.forEach((field) => {
        const values: string[] = valuesFromRecipe[field.name] ?? [];

        values.forEach((value) => {
            filters.push({
                key: uuidv4(),
                rule: field.rule,
                subtype: field.filteringResource,
                value,
            });
        });
    });

    if (filters.length === 0) {
        filters.push(getEmptyFilter());
    }

    return filters;
}

export function getOptionsForTypeSelect(): SelectOption[] {
    return [
        {
            label: 'Include',
            value: 'include',
        },
        {
            label: 'Exclude',
            value: 'exclude',
        },
    ];
}

export function getSubtypeOptions(fields: FilterRecipeField[]): SelectOption[] {
    return [...new Set(fields.map((field) => field.filteringResource))].map((filteringResource) => ({
        label: filteringResource,
        value: filteringResource,
    }));
}

export function filterOutUnsupportedFields(fields: FilterRecipeField[]) {
    let filteredFields = fields;

    if (fields.some((field) => field.type !== FieldType.LIST)) {
        console.warn(
            'Some fields have unsupported type:',
            fields.filter((field) => field.type !== FieldType.LIST),
        );
        filteredFields = fields.filter((field) => field.type === FieldType.LIST);
    }

    return filteredFields;
}

export function convertFiltersToFieldValues(filters: Filter[], fields: FilterRecipeField[]) {
    return fields.reduce((acc, field) => {
        acc[field.name] = filters
            .filter((filter) => filter.rule === field.rule && filter.subtype === field.filteringResource)
            .map((filter) => filter.value);
        return acc;
    }, {});
}
