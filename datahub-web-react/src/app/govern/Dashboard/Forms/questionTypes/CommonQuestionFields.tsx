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
                <Input placeholder="Question title" label="Title" />
            </Form.Item>
            <Form.Item name="description">
                <TextArea label="Description" placeholder="Question description" isDisabled={isFormDisabled} />
            </Form.Item>
        </>
    );
};

export default CommonQuestionFields;
