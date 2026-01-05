import { Input, colors } from '@components';
import { Form } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { RecipeFormItem } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/RecipeFormItem';
import { AddItemButton } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/shared/AddItemButton';
import { HelperText } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/shared/HelperText';
import { RemoveIcon } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/shared/RemoveIcon';
import { CommonFieldProps } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/types';
import { FieldLabel } from '@app/sharedV2/forms/FieldLabel';

const ListWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const SectionWrapper = styled.div`
    align-items: center;
    display: flex;
    padding: 8px 0 8px 12px;
    &:hover {
        background-color: ${colors.gray[1500]};
    }
`;

const FieldsWrapper = styled.div`
    flex: 1;
`;

const StyledRemoveIcon = styled(RemoveIcon)`
    margin: 0 10px;
`;

const ErrorWrapper = styled.div`
    color: ${colors.red[500]};
    margin-top: 5px;
`;

export function DictField({ field }: CommonFieldProps) {
    const helper = field.helper ?? field.tooltip;
    return (
        <Form.List name={field.name} rules={field.rules || undefined}>
            {(fields, { add, remove }, { errors }) => (
                <ListWrapper>
                    <FieldLabel label={field.label} />
                    {fields.map(({ key, name, ...restField }) => (
                        <SectionWrapper key={key}>
                            <FieldsWrapper>
                                {field.keyField && (
                                    <RecipeFormItem
                                        {...restField}
                                        recipeField={field.keyField}
                                        name={[name, field.keyField.name]}
                                        initialValue=""
                                        showHelperText
                                    >
                                        <Input placeholder={field.keyField.placeholder} />
                                    </RecipeFormItem>
                                )}
                                {field.fields?.map((f) => (
                                    <RecipeFormItem
                                        {...restField}
                                        recipeField={f}
                                        name={[name, f.name]}
                                        initialValue=""
                                        showHelperText
                                    >
                                        <Input placeholder={f.placeholder} />
                                    </RecipeFormItem>
                                ))}
                            </FieldsWrapper>
                            <StyledRemoveIcon onClick={() => remove(name)} />
                        </SectionWrapper>
                    ))}

                    <AddItemButton onClick={() => add()} text={field.buttonLabel} />

                    {errors.length > 0 && <ErrorWrapper>{errors}</ErrorWrapper>}
                    {helper && <HelperText text={helper} />}
                </ListWrapper>
            )}
        </Form.List>
    );
}
