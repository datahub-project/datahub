import React from 'react';
import { Button, Col, Form, Input, Row } from 'antd';
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';

export const SpecifyBrowsePath = () => {
    return (
        <>
            <Form.Item label="Specify Browse Location" style={{ marginBottom: 0 }}>
                <Form.List
                    name="browsepathList"
                    rules={[
                        {
                            validator: (_, browsepaths) => {
                                if (browsepaths.length < 1) {
                                    // throw new Error('At least 1 browsepath is needed');
                                    return Promise.reject(new Error('At least 1 Browse Path is needed!'));
                                }
                                if (browsepaths.length > 3) {
                                    // throw new Error('no more than 3 browsepath');
                                    return Promise.reject(new Error('Limited to 3 Browse Paths or less!'));
                                }
                                return Promise.resolve();
                            },
                        },
                    ]}
                >
                    {(fields, { add, remove }, { errors }) => (
                        <>
                            {fields.map((field) => (
                                <Form.Item required key={field.key} name="browsepaths">
                                    <Form.Item
                                        {...field}
                                        validateTrigger={['onChange', 'onBlur']}
                                        rules={[
                                            {
                                                required: true,
                                                pattern: new RegExp(/^\/([0-9a-zA-Z-_ ]+\/){1,6}$/),
                                                message:
                                                    'The path must start and end with a / char, Legal Characters: [a-zA-Z0-9_- ] and the dataset cannot be more than 6 folders deep',
                                            },
                                        ]}
                                        noStyle
                                    >
                                        <Row>
                                            <Col span={16}>
                                                <Input placeholder="browsing path" style={{ width: '100%' }} />
                                            </Col>
                                            <Col span={1}>
                                                {fields.length > 1 ? (
                                                    <Button aria-label="removepath">
                                                        <MinusCircleOutlined
                                                            className="dynamic-delete-button"
                                                            onClick={() => remove(field.name)}
                                                        />
                                                    </Button>
                                                ) : null}
                                            </Col>
                                        </Row>
                                    </Form.Item>
                                </Form.Item>
                            ))}
                            <Form.Item>
                                <Row>
                                    <Col span={16}>
                                        <Button
                                            type="dashed"
                                            style={{ width: '100%' }}
                                            onClick={() => add()}
                                            icon={<PlusOutlined />}
                                            disabled={fields.length >= 3}
                                        >
                                            Add more browsing paths
                                        </Button>
                                        <Form.ErrorList errors={errors} />
                                    </Col>
                                </Row>
                            </Form.Item>
                        </>
                    )}
                </Form.List>
            </Form.Item>
        </>
    );
};
