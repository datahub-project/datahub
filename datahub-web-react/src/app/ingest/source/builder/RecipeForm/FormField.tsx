import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';
import { Checkbox, DatePicker, Form, Input, Select, Tooltip } from 'antd';
import Button from 'antd/lib/button';
import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import DictField, {
    ErrorWrapper,
    Label,
    ListWrapper,
    StyledQuestion,
} from '@app/ingest/source/builder/RecipeForm/DictField';
import SecretField, { StyledFormItem } from '@app/ingest/source/builder/RecipeForm/SecretField/SecretField';
import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';

import { Secret } from '@types';

const StyledButton = styled(Button)`
    color: ${ANTD_GRAY[7]};
    width: 80%;
`;

const StyledRemoveIcon = styled(MinusCircleOutlined)`
    font-size: 14px;
    margin-left: 10px;
`;

interface CommonFieldProps {
    field: RecipeField;
    removeMargin?: boolean;
}

function ListField({ field, removeMargin }: CommonFieldProps) {
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

function SelectField({ field, removeMargin }: CommonFieldProps) {
    return (
        <StyledFormItem
            required={field.required}
            name={field.name}
            label={field.label}
            tooltip={field.tooltip}
            $removeMargin={!!removeMargin}
            rules={field.rules || undefined}
        >
            {field.options && (
                <Select placeholder={field.placeholder} allowClear={!field.required}>
                    {field.options.map((option) => (
                        <Select.Option value={option.value}>{option.label}</Select.Option>
                    ))}
                </Select>
            )}
        </StyledFormItem>
    );
}

function DateField({ field, removeMargin }: CommonFieldProps) {
    return (
        <StyledFormItem
            required={field.required}
            name={field.name}
            label={field.label}
            tooltip={field.tooltip}
            $removeMargin={!!removeMargin}
            rules={field.rules || undefined}
        >
            <DatePicker showTime />
        </StyledFormItem>
    );
}

interface Props {
    field: RecipeField;
    secrets: Secret[];
    refetchSecrets: () => void;
    removeMargin?: boolean;
    updateFormValue: (field, value) => void;
}

function FormField(props: Props) {
    const { field, secrets, refetchSecrets, removeMargin, updateFormValue } = props;

    if (field.type === FieldType.LIST) return <ListField field={field} removeMargin={removeMargin} />;

    if (field.type === FieldType.SELECT) return <SelectField field={field} removeMargin={removeMargin} />;

    if (field.type === FieldType.DATE) return <DateField field={field} removeMargin={removeMargin} />;

    if (field.type === FieldType.SECRET)
        return (
            <SecretField
                field={field}
                secrets={secrets}
                removeMargin={removeMargin}
                refetchSecrets={refetchSecrets}
                updateFormValue={updateFormValue}
            />
        );

    if (field.type === FieldType.DICT) return <DictField field={field} />;

    const isBoolean = field.type === FieldType.BOOLEAN;
    let input = <Input placeholder={field.placeholder} />;
    if (isBoolean) input = <Checkbox />;
    if (field.type === FieldType.TEXTAREA)
        input = <Input.TextArea required={field.required} placeholder={field.placeholder} />;
    const valuePropName = isBoolean ? 'checked' : 'value';
    const getValueFromEvent = isBoolean ? undefined : (e) => (e.target.value === '' ? null : e.target.value);

    return (
        <StyledFormItem
            required={field.required}
            style={isBoolean ? { flexDirection: 'row', alignItems: 'center' } : {}}
            label={field.label}
            name={field.name}
            tooltip={field.tooltip}
            rules={field.rules || undefined}
            valuePropName={valuePropName}
            getValueFromEvent={getValueFromEvent}
            $alignLeft={isBoolean}
            $removeMargin={!!removeMargin}
        >
            {input}
        </StyledFormItem>
    );
}

export default FormField;
