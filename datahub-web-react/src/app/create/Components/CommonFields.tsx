import React from 'react';
import { Form, Input } from 'antd';
import { SpecifyBrowsePath } from './SpecifyBrowsePath';
import { MarkDownEditable } from './MarkDownEditable';
import { DatasetFrequencyInput } from './DatasetFrequencyInput';

export const CommonFields = () => {
    return (
        <>
            <Form.Item
                name="dataset_name"
                label="Dataset Name"
                rules={[
                    {
                        required: true,
                        message: 'Missing dataset name',
                    },
                ]}
            >
                <Input style={{ width: '30%' }} />
            </Form.Item>
            <MarkDownEditable />
            <DatasetFrequencyInput />
            <Form.Item
                name="dataset_origin"
                label="Dataset Origin"
                rules={[
                    {
                        required: false,
                        message: 'Missing dataset origin',
                    },
                ]}
            >
                <Input placeholder="" style={{ width: '80%' }} />
            </Form.Item>
            <Form.Item
                name="dataset_location"
                label="Dataset Location"
                rules={[
                    {
                        required: false,
                        message: 'Missing dataset location',
                    },
                ]}
            >
                <Input placeholder="sample file location" style={{ width: '500px' }} />
            </Form.Item>
            <SpecifyBrowsePath />
        </>
    );
};
