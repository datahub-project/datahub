import React from 'react';
import { Form, Input, Radio, Space } from 'antd';

export const DatasetFrequencyInput = () => {
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
                <Space style={{ marginBottom: 0 }}>
                    <Form.Item>
                        <Radio.Group defaultValue="Onetime" buttonStyle="solid" optionType="button">
                            <Radio.Button value="Onetime">Onetime</Radio.Button>
                            <Radio.Button value="Adhoc">Adhoc</Radio.Button>
                            <Radio.Button value="Periodic">Periodic</Radio.Button>
                            <Radio.Button value="Unknown">Unknown</Radio.Button>
                        </Radio.Group>
                    </Form.Item>
                    <Form.Item name="dataset_frequency_detail">
                        <Input placeholder="Any other details about frequency (freetext)" style={{ width: '990px' }} />
                    </Form.Item>
                </Space>
            </Form.Item>
        </>
    );
};
