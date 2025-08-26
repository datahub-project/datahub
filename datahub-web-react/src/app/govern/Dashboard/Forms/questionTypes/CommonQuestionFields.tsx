import { Form } from 'antd';
import React from 'react';

import { Input, TextArea } from '@src/alchemy-components';

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
                <Input placeholder="Question title" label="Title" data-testid="question-title-input" />
            </Form.Item>
            <Form.Item name="description">
                <TextArea
                    label="Description"
                    placeholder="Question description"
                    isDisabled={isFormDisabled}
                    data-testid="question-description-textarea"
                />
            </Form.Item>
        </>
    );
};

export default CommonQuestionFields;
