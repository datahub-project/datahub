import React, { useState } from 'react';
import axios from 'axios';
import { CSVReader } from 'react-papaparse';
import { Form, Input, Space, Select, Button, message } from 'antd';
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';
import adhocConfig from '../../conf/Adhoc';
import { SearchablePage } from '../search/SearchablePage';

export const AdHocPage = () => {
    const [fileType, setFileType] = useState({ type: 'application/octet-stream' });
    const [form] = Form.useForm();
    const { Option } = Select;
    const formItemLayoutWithOutLabel = {
        wrapperCol: {
            xs: { span: 24, offset: 0 },
            sm: { span: 20, offset: 4 },
        },
    };
    const printSuccessMsg = (status) => {
        message.success(`Status:${status} - Request submitted successfully`, 3).then();
    };
    const printErrorMsg = (error) => {
        message.error(error, 3).then();
    };
    const onFinish = (values) => {
        console.log('Received values of form:', values);
        const finalValue = { ...values, type: fileType };
        console.log('Received finalValue:', finalValue);
        // POST request using axios with error handling
        axios
            .post(adhocConfig, finalValue)
            .then((response) => printSuccessMsg(response.status))
            .catch((error) => {
                printErrorMsg(error.toString());
            });
    };
    const handleOnFileLoad = (data, fileInfo) => {
        console.log('data:', data);
        console.log('fileInfo:', fileInfo);
        // set state for file type
        setFileType(fileInfo.type);
        // get the first row as headers
        if (data.length > 0) {
            // map to array of objects
            const res = data[0].data.map((item) => {
                return { fieldName: item };
            });
            form.setFieldsValue({ fields: res });
        }
    };
    const handleOnRemoveFile = () => {
        form.setFieldsValue({ names: [] });
    };
    const onReset = () => {
        // todo: remove csv file also
        form.resetFields();
    };
    return (
        <>
            <SearchablePage>
                <CSVReader onFileLoad={handleOnFileLoad} addRemoveButton onRemoveFile={handleOnRemoveFile}>
                    <span>Click to upload.</span>
                </CSVReader>
                <br />
                <br />
                <Form
                    form={form}
                    initialValues={{ fields: [{}] }}
                    name="dynamic_form_item"
                    {...formItemLayoutWithOutLabel}
                    onFinish={onFinish}
                >
                    <Form.Item
                        name="datasetName"
                        label="Dataset Name"
                        rules={[
                            {
                                required: true,
                                message: 'Missing dataset name',
                            },
                        ]}
                    >
                        <Input />
                    </Form.Item>
                    <Form.Item
                        name="datasetDescription"
                        label="Dataset Description"
                        rules={[
                            {
                                required: false,
                                message: 'Missing dataset description',
                            },
                        ]}
                    >
                        <Input />
                    </Form.Item>
                    <Form.List name="fields">
                        {(fields, { add, remove }) => (
                            <>
                                {fields.map(({ key, name, fieldKey, ...restField }) => (
                                    <Space key={key} style={{ display: 'flex', marginBottom: 8 }} align="baseline">
                                        <Form.Item
                                            {...restField}
                                            name={[name, 'fieldName']}
                                            fieldKey={[fieldKey, 'fieldName']}
                                            rules={[{ required: true, message: 'Missing field name' }]}
                                        >
                                            <Input placeholder="Field Name" />
                                        </Form.Item>
                                        <Form.Item
                                            {...restField}
                                            name={[name, 'fieldType']}
                                            fieldKey={[fieldKey, 'fieldType']}
                                            rules={[{ required: true, message: 'Missing field type' }]}
                                        >
                                            <Select
                                                showSearch
                                                style={{ width: 200 }}
                                                placeholder="Select field type"
                                                optionFilterProp="children"
                                                filterOption={(input, option) =>
                                                    option?.children.toLowerCase().indexOf(input.toLowerCase()) >= 0
                                                }
                                            >
                                                <Option value="Integer">Integer</Option>
                                                <Option value="String">String</Option>
                                                <Option value="Boolean">Boolean</Option>
                                            </Select>
                                        </Form.Item>
                                        <Form.Item
                                            {...restField}
                                            name={[name, 'fieldDescription']}
                                            fieldKey={[fieldKey, 'fieldDescription']}
                                            rules={[{ required: false, message: 'Missing field description' }]}
                                        >
                                            <Input placeholder="Field Description" />
                                        </Form.Item>
                                        <MinusCircleOutlined onClick={() => remove(name)} />
                                    </Space>
                                ))}
                                <Form.Item>
                                    <Button type="dashed" onClick={() => add()} block icon={<PlusOutlined />}>
                                        Add field
                                    </Button>
                                </Form.Item>
                            </>
                        )}
                    </Form.List>
                    <Form.Item>
                        <Button type="primary" htmlType="submit">
                            Submit
                        </Button>
                        <Button htmlType="button" onClick={onReset}>
                            Reset
                        </Button>
                    </Form.Item>
                </Form>
            </SearchablePage>
        </>
    );
};
