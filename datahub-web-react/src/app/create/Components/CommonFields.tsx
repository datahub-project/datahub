import React from 'react';
import { Form, Input, Tooltip } from 'antd';
// import { SpecifyBrowsePath } from './SpecifyBrowsePath';
import { MarkDownEditable } from './MarkDownEditable';
import { DatasetFrequencyInput } from './DatasetFrequencyInput';

export const CommonFields = () => {
    const aboutName =
        'The dataset name will be appended with a timestamp UUID, for instance datasetName_1654856808524, which is the permanent "key" for the dataset. However, display names can be changed.';
    // const aboutOrigin = 'Where the dataset came from. Is it a derived dataset, etc?';
    // const aboutLocation = 'Location of dataset, or where a sample of it can be found';
    return (
        <>
            <Tooltip trigger="hover" title={aboutName}>
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
            </Tooltip>
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
                <Input style={{ width: '50%' }} />
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
                <Input style={{ width: '50%' }} />
            </Form.Item>
        </>
    );
};
