import React from 'react';
import { Col, Form, Input, Popover, Row } from 'antd';
import { SpecifyBrowsePath } from './SpecifyBrowsePath';
import { MarkDownEditable } from './MarkDownEditable';
import { DatasetFrequencyInput } from './DatasetFrequencyInput';

export const CommonFields = () => {
    const aboutName =
        'The dataset name will be appended with a timestamp UUID, for instance datasetName_1654856808524, which is the permanent "key" for the dataset. However, display names can be changed.';
    const aboutOrigin = 'Where the dataset came from. Is it a derived dataset, etc?';
    const aboutLocation = 'Location of dataset, or where a sample of it can be found';
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
                <Row>
                    <Col span={8} offset={0}>
                        <Popover trigger="hover" content={aboutName}>
                            <Input />
                        </Popover>
                    </Col>
                </Row>
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
                <Row>
                    <Col span={12} offset={0}>
                        <Popover trigger="hover" content={aboutOrigin}>
                            <Input />
                        </Popover>
                    </Col>
                </Row>
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
                <Row>
                    <Col span={12} offset={0}>
                        <Popover trigger="hover" content={aboutLocation}>
                            <Input />
                        </Popover>
                    </Col>
                </Row>
            </Form.Item>
            <Row>
                <Col span={12} offset={3}>
                    <SpecifyBrowsePath />
                </Col>
            </Row>
        </>
    );
};
