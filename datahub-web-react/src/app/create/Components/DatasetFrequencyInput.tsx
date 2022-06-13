import React from 'react';
import { Col, Form, Input, Radio, Row } from 'antd';

export const DatasetFrequencyInput = () => {
    // const style = { background: '#0092ff', padding: '8px 0' };
    return (
        <>
            <Form.Item
                name="dataset_frequency"
                label="Frequency of Data Updates"
                style={{ marginBottom: 0 }}
                rules={[
                    {
                        required: false,
                        message: 'Missing dataset description',
                    },
                ]}
            >
                <Row>
                    <Col span={6} offset={0}>
                        <Form.Item>
                            <Radio.Group defaultValue="Onetime" buttonStyle="solid" optionType="button">
                                <Radio.Button value="Onetime">Onetime</Radio.Button>
                                <Radio.Button value="Adhoc">Adhoc</Radio.Button>
                                <Radio.Button value="Periodic">Periodic</Radio.Button>
                                <Radio.Button value="Unknown">Unknown</Radio.Button>
                            </Radio.Group>
                        </Form.Item>
                    </Col>
                    <Col className="blah" span={14}>
                        <Form.Item name="dataset_frequency_detail">
                            <Input placeholder="Any other details about frequency, for instance, upload date, how frequently updated" />
                        </Form.Item>
                    </Col>
                </Row>
            </Form.Item>
        </>
    );
};
