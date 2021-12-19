import React from 'react';
import { Button, Form, Input } from 'antd';
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';

export const SpecifyBrowsePath = () => {
    const formItemLayout = {
        labelCol: { span: 6 },
        wrapperCol: { span: 14 },
    };
    const buttonItemLayout = {
        wrapperCol: { span: 14, offset: 0 },
    };
    return (
        <>
            <Form.Item label="Specify Browse Location">
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
                                <Form.Item required key={field.key} {...formItemLayout} name="browsepaths">
                                    <Form.Item
                                        {...field}
                                        validateTrigger={['onChange', 'onBlur']}
                                        rules={[
                                            {
                                                required: true,
                                                pattern: new RegExp(/^\/([0-9a-zA-Z_ ]+\/){1,4}$/),
                                                message:
                                                    'The path must start and end with a / char, and the dataset cannot be more than 4 folders deep',
                                            },
                                        ]}
                                        noStyle
                                    >
                                        <Input placeholder="browsing path" style={{ width: '90%' }} />
                                    </Form.Item>
                                    {fields.length > 1 ? (
                                        <Button aria-label="removepath">
                                            <MinusCircleOutlined
                                                className="dynamic-delete-button"
                                                onClick={() => remove(field.name)}
                                            />
                                        </Button>
                                    ) : null}
                                </Form.Item>
                            ))}
                            <Form.Item {...buttonItemLayout}>
                                <Button
                                    type="dashed"
                                    style={{ width: '100%' }}
                                    onClick={() => add()}
                                    icon={<PlusOutlined />}
                                >
                                    Add more browsing paths
                                </Button>
                                <Form.ErrorList errors={errors} />
                            </Form.Item>
                        </>
                    )}
                </Form.List>
            </Form.Item>
        </>
    );
};
