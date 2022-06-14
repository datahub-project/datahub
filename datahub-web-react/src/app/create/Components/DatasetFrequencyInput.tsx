import React from 'react';
import { Col, Form, Input, Popover, Radio, Row } from 'antd';

export const DatasetFrequencyInput = () => {
    // const style = { background: '#0092ff', padding: '8px 0' };
    const aboutOnetime =
        'This dataset is uploaded once and will not be updated. Suggest providing uploaded date if any';
    const aboutAdhoc = 'This dataset is updated on a adhoc basis. Suggest providing details if any';
    const aboutPeriodic = 'This dataset is updated on a periodic basis. Suggest providing details of frequency if any';
    const aboutUnknown = 'Update Frequency is unknown.';
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
                                <Popover trigger="hover" content={aboutOnetime}>
                                    <Radio.Button value="Onetime">Onetime</Radio.Button>
                                </Popover>
                                <Popover trigger="hover" content={aboutAdhoc}>
                                    <Radio.Button value="Adhoc">Adhoc</Radio.Button>
                                </Popover>
                                <Popover trigger="hover" content={aboutPeriodic}>
                                    <Radio.Button value="Periodic">Periodic</Radio.Button>
                                </Popover>
                                <Popover trigger="hover" content={aboutUnknown}>
                                    <Radio.Button value="Unknown">Unknown</Radio.Button>
                                </Popover>
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
