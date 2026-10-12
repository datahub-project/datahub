import { Button as AlchemyButton, Tooltip } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Question } from '@phosphor-icons/react/dist/csr/Question';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import { Button, Form, Input } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { StyledFormItem } from '@app/ingestV2/source/builder/RecipeForm/SecretField/SecretField';
import { RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

export const Label = styled.div`
    font-weight: bold;
    padding-bottom: 8px;
`;

const StyledButton = styled(Button)`
    color: ${(props) => props.theme.colors.textTertiary};
    margin: 10px 0 0 30px;
    width: calc(100% - 72px);
`;

export const StyledQuestion = styled(Question)`
    color: ${(props) => props.theme.colors.icon};
    margin-left: 4px;
`;

export const ListWrapper = styled.div<{ removeMargin: boolean }>`
    margin-bottom: ${(props) => (props.removeMargin ? '0' : '16px')};
`;

const SectionWrapper = styled.div`
    align-items: center;
    display: flex;
    padding: 8px 0 0 30px;
    &:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
`;

const FieldsWrapper = styled.div`
    flex: 1;
`;

const DeleteButton = styled(AlchemyButton)`
    margin-left: 10px;
`;

export const ErrorWrapper = styled.div`
    color: ${(props) => props.theme.colors.textError};
    margin-top: 5px;
`;

interface Props {
    field: RecipeField;
    removeMargin?: boolean;
}

export default function DictField({ field, removeMargin }: Props) {
    const { t } = useTranslation('common.actions');

    return (
        <Form.List name={field.name} rules={field.rules || undefined}>
            {(fields, { add, remove }, { errors }) => (
                <ListWrapper removeMargin={!!removeMargin}>
                    <Label>
                        {field.label}
                        <Tooltip overlay={field.tooltip}>
                            <StyledQuestion />
                        </Tooltip>
                    </Label>
                    {fields.map(({ key, name, ...restField }) => (
                        <SectionWrapper key={key}>
                            <FieldsWrapper>
                                {field.keyField && (
                                    <StyledFormItem
                                        {...restField}
                                        required={field.required}
                                        name={[name, field.keyField.name]}
                                        initialValue=""
                                        label={field.keyField.label}
                                        tooltip={field.keyField.tooltip}
                                        rules={field.keyField.rules || undefined}
                                    >
                                        <Input placeholder={field.keyField.placeholder} />
                                    </StyledFormItem>
                                )}
                                {field.fields?.map((f) => (
                                    <StyledFormItem
                                        {...restField}
                                        name={[name, f.name]}
                                        initialValue=""
                                        label={f.label}
                                        tooltip={f.tooltip}
                                        rules={f.rules || undefined}
                                    >
                                        <Input placeholder={f.placeholder} />
                                    </StyledFormItem>
                                ))}
                            </FieldsWrapper>
                            <DeleteButton
                                variant="text"
                                isCircle
                                color="red"
                                icon={{ icon: Trash, size: 'lg' }}
                                aria-label={t('remove')}
                                onClick={() => remove(name)}
                            />
                        </SectionWrapper>
                    ))}
                    <StyledButton type="dashed" onClick={() => add()} icon={<Plus />}>
                        {field.buttonLabel}
                    </StyledButton>
                    <ErrorWrapper>{errors}</ErrorWrapper>
                </ListWrapper>
            )}
        </Form.List>
    );
}
