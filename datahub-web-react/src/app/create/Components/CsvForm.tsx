import React, { useState } from 'react';
import axios from 'axios';
import { CSVReader } from 'react-papaparse';
import { Form, Input, Space, Select, Button, message, Divider } from 'antd';
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';
import adhocConfig from '../../../conf/Adhoc';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';
import { CommonFields } from './CommonFields';

export const CsvForm = () => {
    const [fileType, setFileType] = useState({ dataset_type: 'application/octet-stream' });
    const user = useGetAuthenticatedUser();
    const printSuccessMsg = (status) => {
        message.success(`Status:${status} - Request submitted successfully`, 3).then();
    };
    const printErrorMsg = (error) => {
        message.error(error, 3).then();
    };
    const [form] = Form.useForm();
    const { Option } = Select;
    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 14,
        },
    };
    const onFinish = (values) => {
        console.log('Received values of form:', values);
        const finalValue = { ...values, ...fileType, dataset_owner: user?.username };
        console.log('Received finalValue:', finalValue);
        // POST request using axios with error handling
        axios
            .post(adhocConfig, finalValue)
            .then((response) => printSuccessMsg(response.status))
            .catch((error) => {
                printErrorMsg(error.toString());
            });
    };
    const onReset = () => {
        // todo: remove csv file also
        form.resetFields();
    };
    const handleOnFileLoad = (data, fileInfo) => {
        console.log('data:', data);
        console.log('fileInfo:', fileInfo);
        // set state for file type
        setFileType({ dataset_type: fileInfo.type });
        // get the first row as headers
        if (data.length > 0) {
            // map to array of objects
            const res = data[0].data.map((item) => {
                return { field_name: item, field_description: '' };
            });
            form.setFieldsValue({ fields: res });
        }
    };
    const handleOnRemoveFile = () => {
        form.setFieldsValue({ fields: [{ field_description: '' }] });
    };
    return (
        <>
            <Form
                {...layout}
                form={form}
                initialValues={{ fields: [{ field_description: '' }] }}
                name="dynamic_form_item"
                onFinish={onFinish}
            >
                <CSVReader onFileLoad={handleOnFileLoad} addRemoveButton onRemoveFile={handleOnRemoveFile}>
                    <span>Click here to parse your file header (CSV or delimited text file only)</span>
                </CSVReader>
                <Divider dashed orientation="left">
                    Dataset Info
                </Divider>
                <CommonFields />
                <Form.Item label="Dataset Fields" name="fields">
                    <Form.List {...layout} name="fields">
                        {(fields, { add, remove }) => (
                            <>
                                {fields.map(({ key, name, fieldKey, ...restField }) => (
                                    <Space key={key} style={{ display: 'flex', marginBottom: 8 }} align="baseline">
                                        <Form.Item
                                            {...restField}
                                            name={[name, 'field_name']}
                                            fieldKey={[fieldKey, 'field_name']}
                                            rules={[{ required: true, message: 'Missing field name' }]}
                                        >
                                            <Input placeholder="Field Name" />
                                        </Form.Item>
                                        <Form.Item
                                            {...restField}
                                            name={[name, 'field_type']}
                                            fieldKey={[fieldKey, 'field_type']}
                                            rules={[{ required: true, message: 'Missing field type' }]}
                                        >
                                            <Select showSearch style={{ width: 150 }} placeholder="Select field type">
                                                <Option value="num">Number</Option>
                                                <Option value="string">String</Option>
                                                <Option value="bool">Boolean</Option>
                                            </Select>
                                        </Form.Item>
                                        <Form.Item
                                            {...restField}
                                            name={[name, 'field_description']}
                                            fieldKey={[fieldKey, 'field_description']}
                                            rules={[
                                                {
                                                    required: false,
                                                    message: 'Missing field description',
                                                },
                                            ]}
                                        >
                                            <Input style={{ width: 200 }} placeholder="Field Description" />
                                        </Form.Item>
                                        {fields.length > 1 ? (
                                            <MinusCircleOutlined onClick={() => remove(name)} />
                                        ) : null}
                                    </Space>
                                ))}
                                <Form.Item>
                                    <Button
                                        type="dashed"
                                        onClick={() => add({ field_description: '' })}
                                        block
                                        icon={<PlusOutlined />}
                                    >
                                        Add field
                                    </Button>
                                </Form.Item>
                            </>
                        )}
                    </Form.List>
                    <Space>
                        <Button type="primary" htmlType="submit">
                            Submit
                        </Button>
                        <Button htmlType="button" onClick={onReset}>
                            Reset
                        </Button>
                    </Space>
                </Form.Item>
            </Form>
        </>
    );
};
