import React from 'react';
import { Button, Form, Input, Tooltip } from 'antd';
import { red } from '@ant-design/colors';
import styled from 'styled-components/macro';
import { DeleteOutlined, PlusOutlined, QuestionCircleOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { RecipeField } from './common';
import { StyledFormItem } from './SecretField/SecretField';

export const Label = styled.div`
    font-weight: bold;
    padding-bottom: 8px;
`;

export const StyledButton = styled(Button)`
    color: ${ANTD_GRAY[7]};
    margin: 10px 0 0 30px;
    width: calc(100% - 72px);
`;

export const StyledQuestion = styled(QuestionCircleOutlined)`
    color: rgba(0, 0, 0, 0.45);
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
        background-color: ${ANTD_GRAY[2]};
    }
`;

const FieldsWrapper = styled.div`
    flex: 1;
`;

const StyledDeleteButton = styled(Button)`
    margin-left: 10px;
`;

export const ErrorWrapper = styled.div`
    color: ${red[5]};
    margin-top: 5px;
`;

interface Props {
    field: RecipeField;
    removeMargin?: boolean;
}

export default function DictField({ field, removeMargin }: Props) {
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
                            <StyledDeleteButton onClick={() => remove(name)} type="text" shape="circle" danger>
                                <DeleteOutlined />
                            </StyledDeleteButton>
                        </SectionWrapper>
                    ))}
                    <StyledButton type="dashed" onClick={() => add()} icon={<PlusOutlined />}>
                        {field.buttonLabel}
                    </StyledButton>
                    <ErrorWrapper>{errors}</ErrorWrapper>
                </ListWrapper>
            )}
        </Form.List>
    );
}
