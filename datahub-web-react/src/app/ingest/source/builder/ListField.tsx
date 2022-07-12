import { Button, Form, Input } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';

const Label = styled.div`
    font-weight: bold;
    padding-bottom: 8px;
`;

const StyledRemoveIcon = styled(MinusCircleOutlined)`
    font-size: 14px;
    margin-left: 10px;
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

export default ListField;
