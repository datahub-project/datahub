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

const StyledFormItem = styled(Form.Item)<{ alignLeft: boolean; removeMargin: boolean }>`
    margin-bottom: ${(props) => (props.removeMargin ? '0' : '16px')};

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

const ListWrapper = styled.div<{ removeMargin: boolean }>`
    margin-bottom: ${(props) => (props.removeMargin ? '0' : '16px')};
`;

interface ListFieldProps {
    field: RecipeField;
    removeMargin?: boolean;
}

function ListField({ field, removeMargin }: ListFieldProps) {
    return (
        <Form.List name={field.name}>
            {(fields, { add, remove }) => (
                <ListWrapper removeMargin={!!removeMargin}>
                    <Label>{field.label}</Label>
                    {fields.map((item) => (
                        <Form.Item key={item.fieldKey} style={{ marginBottom: '10px' }}>
                            <Form.Item {...item} noStyle>
                                <Input style={{ width: '80%' }} />
                            </Form.Item>
                            <StyledRemoveIcon onClick={() => remove(item.name)} />
                        </Form.Item>
                    ))}
                    <Button type="dashed" onClick={() => add()} style={{ width: '80%' }} icon={<PlusOutlined />}>
                        Add pattern
                    </Button>
                </ListWrapper>
            )}
        </Form.List>
    );
}

interface Props {
    field: RecipeField;
    removeMargin?: boolean;
}

function FormField(props: Props) {
    const { field, removeMargin } = props;

    if (field.type === FieldType.LIST) return <ListField field={field} removeMargin={removeMargin} />;

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
            removeMargin={!!removeMargin}
        >
            {input}
        </StyledFormItem>
    );
}

export default FormField;
