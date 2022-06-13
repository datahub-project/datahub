import React from 'react';
import { Col, Form, Input, Row } from 'antd';
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
                <Row>
                    <Col span={8} offset={0}>
                        <Input />
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
                        <Input placeholder="" />
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
                        <Input placeholder="sample file location" />
                    </Col>
                </Row>
            </Form.Item>
            <SpecifyBrowsePath />
        </>
    );
};
