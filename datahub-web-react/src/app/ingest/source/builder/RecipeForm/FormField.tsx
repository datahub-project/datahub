import React from 'react';
import { Checkbox, Form, Input, Select, Tooltip } from 'antd';
import styled from 'styled-components/macro';
import Button from 'antd/lib/button';
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';
import { RecipeField, FieldType } from './common';
import { Secret } from '../../../../../types.generated';
import SecretField, { StyledFormItem } from './SecretField/SecretField';
import DictField, { Label, StyledQuestion, ListWrapper, ErrorWrapper } from './DictField';
import { ANTD_GRAY } from '../../../../entity/shared/constants';

const StyledButton = styled(Button)`
    color: ${ANTD_GRAY[7]};
    width: 80%;
`;

const StyledRemoveIcon = styled(MinusCircleOutlined)`
    font-size: 14px;
    margin-left: 10px;
`;

interface ListFieldProps {
    field: RecipeField;
    removeMargin?: boolean;
}

interface SelectFieldProps {
    field: RecipeField;
    removeMargin?: boolean;
}

function ListField({ field, removeMargin }: ListFieldProps) {
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
                    {fields.map((item) => (
                        <Form.Item key={item.fieldKey} style={{ marginBottom: '10px' }}>
                            <Form.Item {...item} noStyle>
                                <Input style={{ width: '80%' }} placeholder={field.placeholder} />
                            </Form.Item>
                            <StyledRemoveIcon onClick={() => remove(item.name)} />
                        </Form.Item>
                    ))}
                    <StyledButton type="dashed" onClick={() => add()} style={{ width: '80%' }} icon={<PlusOutlined />}>
                        {field.buttonLabel}
                    </StyledButton>
                    <ErrorWrapper>{errors}</ErrorWrapper>
                </ListWrapper>
            )}
        </Form.List>
    );
}

function SelectField({ field, removeMargin }: SelectFieldProps) {
    return (
        <StyledFormItem
            name={field.name}
            label={field.label}
            tooltip={field.tooltip}
            removeMargin={!!removeMargin}
            rules={field.rules || undefined}
        >
            {field.options && (
                <Select placeholder={field.placeholder}>
                    {field.options.map((option) => (
                        <Select.Option value={option.value}>{option.label}</Select.Option>
                    ))}
                </Select>
            )}
        </StyledFormItem>
    );
}

interface Props {
    field: RecipeField;
    secrets: Secret[];
    refetchSecrets: () => void;
    removeMargin?: boolean;
}

function FormField(props: Props) {
    const { field, secrets, refetchSecrets, removeMargin } = props;

    if (field.type === FieldType.LIST) return <ListField field={field} removeMargin={removeMargin} />;

    if (field.type === FieldType.SELECT) return <SelectField field={field} removeMargin={removeMargin} />;

    if (field.type === FieldType.SECRET)
        return (
            <SecretField field={field} secrets={secrets} removeMargin={removeMargin} refetchSecrets={refetchSecrets} />
        );

    if (field.type === FieldType.DICT) return <DictField field={field} />;

    const isBoolean = field.type === FieldType.BOOLEAN;
    const input = isBoolean ? <Checkbox /> : <Input placeholder={field.placeholder} />;
    const valuePropName = isBoolean ? 'checked' : 'value';
    const getValueFromEvent = isBoolean ? undefined : (e) => (e.target.value === '' ? null : e.target.value);

    return (
        <StyledFormItem
            style={isBoolean ? { flexDirection: 'row', alignItems: 'center' } : {}}
            label={field.label}
            name={field.name}
            tooltip={field.tooltip}
            rules={field.rules || undefined}
            valuePropName={valuePropName}
            getValueFromEvent={getValueFromEvent}
            alignLeft={isBoolean}
            removeMargin={!!removeMargin}
        >
            {input}
        </StyledFormItem>
    );
}

export default FormField;
