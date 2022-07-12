import { Button, Form, Input } from 'antd';
import React from 'react';
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';

function ListField(inputField: any) {
    return (
        <Form.List name={inputField.field.name}>
            {(fields, { add, remove }) => (
                <>
                    <>
                        {inputField.field.label}
                        {fields.map((field) => (
                            <Form.Item key={field.fieldKey}>
                                <Form.Item {...field} validateTrigger={['onChange', 'onBlur']}>
                                    <Input
                                        style={{
                                            width: '60%',
                                        }}
                                    />
                                </Form.Item>
                                <MinusCircleOutlined onClick={() => remove(field.name)} />
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
                            Add field
                        </Button>
                    </>
                </>
            )}
        </Form.List>
    );
}

export default ListField;
