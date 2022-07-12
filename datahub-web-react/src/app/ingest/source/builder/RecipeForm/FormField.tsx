import { Button, Checkbox, Form, Input } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';
import { FieldType, RecipeField } from './constants';

const Label = styled.div`
    font-weight: bold;
    padding-bottom: 8px;
`;

const StyledRemoveIcon = styled(MinusCircleOutlined)`
    font-size: 14px;
    margin-left: 10px;
`;

const StyledFormItem = styled(Form.Item)<{ alignLeft: boolean }>`
    ${(props) =>
        props.alignLeft &&
        `
        .ant-form-item {
            flex-direction: row;

        }

        .ant-form-item-label {
            padding: 0;
            margin-right: 10px;
        }
    `}
`;

function ListField(inputField: any) {
    return (
        <Form.List name={inputField.field.name}>
            {(fields, { add, remove }) => (
                <>
                    <>
                        <Label>{inputField.field.label}</Label>
                        {fields.map((field) => (
                            <Form.Item key={field.fieldKey} style={{ marginBottom: '10px' }}>
                                <Form.Item {...field} noStyle>
                                    <Input
                                        style={{
                                            width: '60%',
                                        }}
                                    />
                                </Form.Item>
                                <StyledRemoveIcon onClick={() => remove(field.name)} />
                            </Form.Item>
                        ))}
                        <Button
                            type="dashed"
                            onClick={() => add()}
                            style={{
                                width: '60%',
                            }}
                            icon={<PlusOutlined />}
                        >
                            Add pattern
                        </Button>
                    </>
                </>
            )}
        </Form.List>
    );
}

interface Props {
    field: RecipeField;
}

function FormField(props: Props) {
    const { field } = props;

    if (field.type === FieldType.LIST) return <ListField field={field} />;

    const isBoolean = field.type === FieldType.BOOLEAN;
    const input = isBoolean ? <Checkbox /> : <Input />;
    const valuePropName = isBoolean ? 'checked' : 'value';
    const getValueFromEvent = isBoolean ? undefined : (e) => (e.target.value === '' ? undefined : e.target.value);

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
        >
            {input}
        </StyledFormItem>
    );
}

export default FormField;
