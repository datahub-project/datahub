import { Input, TextArea } from '@src/alchemy-components';
import { Form } from 'antd';
import React from 'react';

interface Props {
    isFormDisabled: boolean;
}

const CommonQuestionFields = ({ isFormDisabled }: Props) => {
    return (
        <>
            <Form.Item
                name="title"
                rules={[
                    {
                        required: true,
                        message: 'Please enter the question',
                    },
                ]}
            >
                <Input placeholder="Add Question here" label="Title" />
            </Form.Item>
            <Form.Item name="description">
                <TextArea label="Description" placeholder="Add description here" isDisabled={isFormDisabled} />
            </Form.Item>
        </>
    );
};

export default CommonQuestionFields;
