import { Typography } from 'antd';
import React, { Fragment } from 'react';

import { RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';
import { FormField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/FormField';

interface Props {
    filterFields?: RecipeField[];
    updateFormValue: (field, value) => void;
}

function shouldRenderFilterSectionHeader(field: RecipeField, index: number, filterFields: RecipeField[]) {
    if (index === 0 && field.section) return true;
    if (field.section && filterFields[index - 1].section !== field.section) return true;
    return false;
}

export function FiltersSection({ filterFields, updateFormValue }: Props) {
    if (!filterFields || filterFields.length === 0) return null;

    return (
        <>
            {filterFields.map((field, i) => (
                <Fragment key={field.name}>
                    {shouldRenderFilterSectionHeader(field, i, filterFields) && (
                        <Typography.Title level={4}>{field.section}</Typography.Title>
                    )}
                    <FormField field={field} updateFormValue={updateFormValue} />
                </Fragment>
            ))}
        </>
    );
}
