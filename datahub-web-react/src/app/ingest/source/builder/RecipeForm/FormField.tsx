import React from 'react';
import { Button, Checkbox, Form, Input, Select, Tooltip } from 'antd';
import styled from 'styled-components/macro';
import { MinusCircleOutlined, PlusOutlined, QuestionCircleOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { RecipeField, FieldType } from './common';
import { Secret } from '../../../../../types.generated';
import SecretField, { StyledFormItem } from './SecretField/SecretField';

const Label = styled.div`
    font-weight: bold;
    padding-bottom: 8px;
`;

const StyledButton = styled(Button)`
    color: ${ANTD_GRAY[7]};
    width: 80%;
`;

const StyledQuestion = styled(QuestionCircleOutlined)`
    color: rgba(0, 0, 0, 0.45);
    margin-left: 4px;
`;

const StyledRemoveIcon = styled(MinusCircleOutlined)`
    font-size: 14px;
    margin-left: 10px;
`;

const ListWrapper = styled.div<{ removeMargin: boolean }>`
    margin-bottom: ${(props) => (props.removeMargin ? '0' : '16px')};
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
        <Form.List name={field.name}>
            {(fields, { add, remove }) => (
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
                </ListWrapper>
            )}
        </Form.List>
    );
}

function SelectField({ field, removeMargin }: SelectFieldProps) {
    return (
        <StyledFormItem name={field.name} label={field.label} tooltip={field.tooltip} removeMargin={!!removeMargin}>
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
