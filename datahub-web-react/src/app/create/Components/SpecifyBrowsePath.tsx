import React from 'react';
import { Button, Col, Form, Input, Row } from 'antd';
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';

export const SpecifyBrowsePath = () => {
    // const aboutBrowsePath =
    //     'BrowsePath affects where the dataset is located when user browses datasets. BrowsePath must start and end with a /';
    return (
        <>
            <Form.Item label="Specify Browse Location" style={{ marginBottom: 0 }} name="browsepathList">
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
                            {fields.map(({ key, name, fieldKey, ...restField }) => (
                                <Row key={key}>
                                    <Col span={6}>
                                        <Form.Item
                                            {...restField}
                                            name={[name, 'browsepath']}
                                            fieldKey={[fieldKey, 'browsepath']}
                                            validateTrigger={['onChange', 'onBlur']}
                                            rules={[
                                                {
                                                    required: true,
                                                    pattern: new RegExp(/^\/([0-9a-zA-Z-_ ]+\/){1,6}$/),
                                                    message:
                                                        'The path must start and end with a / char, Legal Characters: [a-zA-Z0-9_- ] and the dataset cannot be more than 6 folders deep',
                                                },
                                            ]}
                                        >
                                            <Input />
                                        </Form.Item>
                                    </Col>
                                    <Col span={1}>
                                        <Form.Item>
                                            {fields.length > 1 ? (
                                                <Button aria-label="removepath">
                                                    <MinusCircleOutlined
                                                        className="dynamic-delete-button"
                                                        onClick={() => remove(name)}
                                                    />
                                                </Button>
                                            ) : null}
                                        </Form.Item>
                                    </Col>
                                </Row>
                            ))}
                            <Col span={7}>
                                <Form.Item label="">
                                    <Button
                                        onClick={() => add()}
                                        icon={<PlusOutlined />}
                                        disabled={fields.length >= 3}
                                        style={{ width: '100%' }}
                                    >
                                        Add more browsing paths
                                    </Button>
                                    <Form.ErrorList errors={errors} />
                                </Form.Item>
                            </Col>
                        </>
                    )}
                </Form.List>
            </Form.Item>
        </>
    );
};
