import React, { useState } from 'react';
import axios from 'axios';
import { CSVReader } from 'react-papaparse';
import { Form, Input, Space, Select, Button, message, Divider, InputNumber } from 'antd';
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';
// import { gql, useQuery } from '@apollo/client';
import { CommonFields } from './CommonFields';
// import adhocConfig from '../../../conf/Adhoc';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';
import { GetMyToken } from '../../entity/dataset/whoAmI';

export const CsvForm = () => {
    // this wacky setup is because the URL is different when running docker-compose vs Ingress
    // for docker-compose, need to change port. For ingress, just modify subpath will do.
    // having a setup that works for both makes development easier.
    const initialUrl = window.location.href;
    let publishUrl = initialUrl.includes(':3000')
        ? initialUrl.replace(':3000/adhoc/', ':8001/custom/make_dataset')
        : initialUrl;
    publishUrl = publishUrl.includes(':9002') 
        ? publishUrl.replace(':9002/adhoc/', ':8001/custom/make_dataset') 
        : publishUrl.replace('/adhoc/', '/custom/make_dataset');

    console.log(`the publish url is ${publishUrl}`);
    const user = useGetAuthenticatedUser();
    const userUrn = user?.corpUser?.urn || '';
    const userToken = GetMyToken(userUrn);
    // console.log(`user is ${userUrn} and token is ${userToken}, received at ${Date().toLocaleString()}`);
    const [fileType, setFileType] = useState({ dataset_type: 'application/octet-stream' });
    const [hasHeader, setHasHeader] = useState('no');

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
        // console.log('Received values of form:', values);
        const finalValue = { ...values, ...fileType, dataset_owner: user?.corpUser?.username, user_token: userToken };
        // console.log('Received finalValue:', finalValue);
        // POST request using axios with error handling
        console.log(`publish is now ${publishUrl}`);
        axios
            .post(publishUrl, finalValue)
            .then((response) => printSuccessMsg(response.status))
            .catch((error) => {
                printErrorMsg(error.toString());
            });
    };
    const onReset = () => {
        form.resetFields();
    };
    const onHeaderChange = (value) => {
        setHasHeader(value);
    };
    const handleOnFileLoad = (data, fileInfo) => {
        console.log('data:', data);
        console.log('fileInfo:', fileInfo);
        let headerLine = form.getFieldValue('headerLine');
        console.log('form values', form.getFieldValue('headerLine'));
        // set state for file type
        setFileType({ dataset_type: fileInfo.type });
        // get the first row as headers
        if (data.length > 0 && headerLine <= data.length) {
            // map to array of objects
            const res = data[--headerLine].data.map((item) => {
                return { field_name: item, field_description: '' };
            });
            form.setFieldsValue({ fields: res, hasHeader: 'yes' });
            setHasHeader('yes');
        } else {
            message.error('Empty file or invalid header line', 3).then();
        }
    };
    const handleOnRemoveFile = () => {
        form.resetFields();
        setHasHeader('no');
    };
    return (
        <>
            <Form
                {...layout}
                form={form}
                initialValues={{
                    fields: [{ field_description: '', field_type: 'string' }],
                    hasHeader: 'no',
                    headerLine: 1,
                    browsepathList: ['/csv/'],
                }}
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
                <Form.Item
                    name="hasHeader"
                    label="File Header"
                    rules={[
                        {
                            required: true,
                        },
                    ]}
                >
                    <Select placeholder="Does the file contains header" onChange={onHeaderChange} data-testid="select">
                        <Option value="yes">Yes</Option>
                        <Option value="no">No</Option>
                    </Select>
                </Form.Item>
                {hasHeader === 'yes' && (
                    <Form.Item label="Header Line" name="headerLine">
                        <InputNumber min={1} max={10} />
                    </Form.Item>
                )}
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
                                                <Option value="double">Double</Option>
                                                <Option value="string">String</Option>
                                                <Option value="bool">Boolean</Option>
                                                <Option value="date-time">Datetime</Option>
                                                <Option value="date">Date</Option>
                                                <Option value="time">Time</Option>
                                                <Option value="bytes">Bytes</Option>
                                                <Option value="unknown">Unknown</Option>
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
                                            <MinusCircleOutlined
                                                data-testid="delete-icon"
                                                onClick={() => remove(name)}
                                            />
                                        ) : null}
                                    </Space>
                                ))}
                                <Form.Item>
                                    <Button
                                        type="dashed"
                                        onClick={() => add({ field_description: '', field_type: 'string' })}
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
