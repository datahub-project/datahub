import { Text } from '@components';
import { PromptCardinality } from '@src/types.generated';
import { Form, Radio } from 'antd';
import React from 'react';
import { FieldLabel } from '../styledComponents';

const OwnershipQuestion = () => {
    return (
        <>
            <FieldLabel> Choose type of ownership</FieldLabel>
            <Form.Item
                name={['ownershipParams', 'cardinality']}
                rules={[
                    {
                        required: true,
                        message: 'Please select the type of ownership',
                    },
                ]}
            >
                <Radio.Group>
                    <Radio value={PromptCardinality.Single}>
                        <Text color="gray"> Single owner</Text>
                    </Radio>
                    <Radio value={PromptCardinality.Multiple}>
                        <Text color="gray">Multiple owners</Text>
                    </Radio>
                </Radio.Group>
            </Form.Item>
        </>
    );
};

export default OwnershipQuestion;
