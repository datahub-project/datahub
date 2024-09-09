import { TextArea } from '@src/alchemy-components';
import { Form, Input, Radio } from 'antd';
import React from 'react';
import { FieldLabel } from '../styledComponents';

interface Props {
    isFormDisabled: boolean;
}

const CommonQuestionFields = ({ isFormDisabled }: Props) => {
    return (
        <>
            <FieldLabel> Question</FieldLabel>
            <Form.Item
                name="title"
                rules={[
                    {
                        required: true,
                        message: 'Please enter the question',
                    },
                ]}
            >
                <Input placeholder="Add Question here" />
            </Form.Item>
            <Form.Item name="description">
                <TextArea label="Description" placeholder="Add description here" isDisabled={isFormDisabled} />
            </Form.Item>
            <FieldLabel> Mandatory</FieldLabel>
            <Form.Item name="required">
                <Radio.Group defaultValue={false}>
                    <Radio value>Yes</Radio>
                    <Radio value={false}>No</Radio>
                </Radio.Group>
            </Form.Item>
        </>
    );
};

export default CommonQuestionFields;
