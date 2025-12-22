import { Input, spacing } from '@components';
import { Form } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { ErrorWrapper } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/ErrorWrapper';
import { RecipeFormItem } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/RecipeFormItem';
import { AddItemButton } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/shared/AddItemButton';
import { HelperText } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/shared/HelperText';
import { RemoveIcon } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/shared/RemoveIcon';
import { CommonFieldProps } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/types';
import { FieldLabel } from '@app/sharedV2/forms/FieldLabel';

const ListWrapper = styled.div`
    margin-bottom: 16px;
    display: flex;
    flex-direction: column;
    gap: ${spacing.sm};
`;

const ListItemWrapper = styled.div`
    display: flex;
    flex-direction: row;
    gap: ${spacing.sm};
    align-items: center;
    width: 100%;
`;

export function ListField({ field }: CommonFieldProps) {
    return (
        <Form.List name={field.name} rules={field.rules || undefined}>
            {(fields, { add, remove }, { errors }) => (
                <ListWrapper>
                    <FieldLabel label={field.label} required={field.required} />

                    {fields.map((item) => (
                        <ListItemWrapper key={item.fieldKey}>
                            <RecipeFormItem {...item} initialValue="" noStyle>
                                <Input placeholder={field.placeholder} style={{ width: '100%' }} />
                            </RecipeFormItem>
                            <RemoveIcon onClick={() => remove(item.name)} />
                        </ListItemWrapper>
                    ))}

                    <AddItemButton onClick={() => add()} text={field.buttonLabel} />

                    {errors.length > 0 && <ErrorWrapper errors={errors} />}
                    {field.helper && <HelperText text={field.helper} />}
                </ListWrapper>
            )}
        </Form.List>
    );
}
