import React, { useState } from 'react';
import axios from 'axios';
import { CSVReader } from 'react-papaparse';
import { Col, Form, Input, Space, Select, Button, message, Divider, Popconfirm, Row, Tooltip } from 'antd';
import { MinusCircleOutlined, PlusOutlined, AlertOutlined } from '@ant-design/icons';
import { CommonFields } from './CommonFields';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';
import { GetMyToken } from '../../entity/dataset/whoAmI';
import { WhereAmI } from '../../home/whereAmI';
import { DataPlatformSelect } from '../../entity/shared/tabs/Dataset/platformSelect/DataPlatformSelect';
import { printErrorMsg, printSuccessMsg } from '../../entity/shared/tabs/Dataset/ApiCallUtils';

export const CsvForm = () => {
    const urlBase = WhereAmI();
    const publishUrl = `${urlBase}custom/make_dataset`;
    const [visible, setVisible] = useState(false);
    const [confirmLoading, setConfirmLoading] = useState(false);
    console.log(`the publish url is ${publishUrl}`);
    const user = useGetAuthenticatedUser();
    const userUrn = user?.corpUser?.urn || '';
    const userToken = GetMyToken(userUrn);
    const aboutType = 'Specify the datatype for field. If unsure, set "unknown"';
    // const aboutDesc = 'Field description. Can accept markdown as well, except there is no preview in this form.';
    // const [fileType, setFileType] = useState({ dataset_type: 'application/octet-stream' });

    const [form] = Form.useForm();
    const { Option } = Select;
    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 18,
        },
    };

    function showPopconfirm() {
        setVisible(true);
    }
    const popupHandleOk = () => {
        setConfirmLoading(true);
        const values = form.getFieldsValue();
        console.log(`the values received are ${Object.values(values)}`);
        const finalValue = { ...values, dataset_owner: user?.corpUser?.username, user_token: userToken };
        axios
            .post(publishUrl, finalValue)
            .then((response) => printSuccessMsg(response.status))
            .catch((error) => {
                printErrorMsg(error.toString());
            });
        setTimeout(() => {
            setVisible(false);
            setConfirmLoading(false);
        }, 3000);
    };
    const popuphandleCancel = () => {
        setVisible(false);
    };
    const onReset = () => {
        form.resetFields();
    };
    const handleOnFileLoad = (data, fileInfo) => {
        console.log('data:', data);
        console.log('fileInfo:', fileInfo);
        const headerLine = 0;
        console.log('form values', form.getFieldValue('headerLine'));
        // set state for file type
        // setFileType({ dataset_type: fileInfo.type });
        // get the first row as headers
        if (data.length > 0 && headerLine <= data.length) {
            // map to array of objects
            const res = data[headerLine].data.map((item) => {
                return { field_name: item, field_description: '' };
            });
            form.setFieldsValue({ fields: res });
            console.log(`res is ${res}`);
        } else {
            message.error('Empty file or invalid header line', 3).then();
        }
    };
    const handleOnRemoveFile = () => {
        form.resetFields();
    };
    const validateForm = () => {
        form.validateFields().then(() => {
            showPopconfirm();
        });
    };
    const popupMsg = `Confirm Dataset Name is correct: ${form.getFieldValue('dataset_name')}? 
    This will permanently affect the dataset URL`;
    // const { TextArea } = Input;
    return (
        <>
            <Form
                {...layout}
                form={form}
                initialValues={{
                    dataset_name: 'abc',
                    fields: [{ field_description: '', field_type: 'string' }],
                    browsepathList: ['/file/'],
                    frequency: 'Unknown',
                    dataset_origin: 'dunno',
                    platformSelect: 'urn:li:dataPlatform:elasticsearch',
                }}
                name="dynamic_form_item"
            >
                <CSVReader onFileLoad={handleOnFileLoad} addRemoveButton onRemoveFile={handleOnRemoveFile}>
                    <span>
                        Click here to pre-generate dictionary from your data file header (CSV or delimited text file
                        only)
                    </span>
                </CSVReader>
                <Divider dashed orientation="left">
                    Dataset Info
                </Divider>
                <DataPlatformSelect />
                <CommonFields />
                <Form.Item label="Dataset Fields" name="fields">
                    <Form.List name="fields">
                        {(fields, { add, remove }) => (
                            <>
                                {fields.map(({ key, name, fieldKey, ...restField }) => (
                                    <Row key={key}>
                                        <Col span={4} offset={0}>
                                            <Form.Item
                                                {...restField}
                                                name={[name, 'field_name']}
                                                fieldKey={[fieldKey, 'field_name']}
                                                rules={[{ required: true, message: 'Missing field name' }]}
                                            >
                                                <Input placeholder="Field Name" />
                                            </Form.Item>
                                        </Col>
                                        <Col span={2}>
                                            <Tooltip trigger="hover" title={aboutType}>
                                                <Form.Item
                                                    {...restField}
                                                    name={[name, 'field_type']}
                                                    fieldKey={[fieldKey, 'field_type']}
                                                    rules={[{ required: true, message: 'Missing field type' }]}
                                                >
                                                    {/* <Popover trigger="hover" content={aboutType}> */}
                                                    <Select showSearch placeholder="Select field type">
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
                                                    {/* </Popover> */}
                                                </Form.Item>
                                            </Tooltip>
                                        </Col>
                                        <Col span={14}>
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
                                                {/* <Popover trigger="hover" content={aboutDesc}> */}
                                                <Input placeholder="Field Name" />
                                                {/* <TextArea
                                                        rows={4}
                                                        placeholder="Field Description"
                                                        autoSize={{ minRows: 1, maxRows: 3 }}
                                                    /> */}
                                                {/* </Popover> */}
                                            </Form.Item>
                                        </Col>
                                        <Col span={1}>
                                            {fields.length > 1 ? (
                                                <Form.Item>
                                                    <Button>
                                                        <MinusCircleOutlined
                                                            data-testid="delete-icon"
                                                            onClick={() => remove(name)}
                                                        />
                                                    </Button>
                                                </Form.Item>
                                            ) : null}
                                        </Col>
                                    </Row>
                                ))}
                                <Row>
                                    <Col span={20} offset={0}>
                                        <Form.Item>
                                            <Button
                                                type="dashed"
                                                onClick={() =>
                                                    add({
                                                        field_description: '',
                                                        field_type: 'string',
                                                    })
                                                }
                                                block
                                                icon={<PlusOutlined />}
                                            >
                                                Add field
                                            </Button>
                                        </Form.Item>
                                    </Col>
                                </Row>
                            </>
                        )}
                    </Form.List>
                    <Space>
                        <Popconfirm
                            title={popupMsg}
                            visible={visible}
                            onConfirm={popupHandleOk}
                            okButtonProps={{ loading: confirmLoading }}
                            onCancel={popuphandleCancel}
                            icon={<AlertOutlined style={{ color: 'red' }} />}
                        >
                            <Button htmlType="button" onClick={validateForm}>
                                Submit
                            </Button>
                        </Popconfirm>
                        <Button htmlType="button" onClick={onReset}>
                            Reset
                        </Button>
                    </Space>
                </Form.Item>
            </Form>
        </>
    );
};
