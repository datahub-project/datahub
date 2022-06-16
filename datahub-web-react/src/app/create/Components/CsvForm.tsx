import React, { useState } from 'react';
import axios from 'axios';
import { CSVReader } from 'react-papaparse';
import { Col, Form, Input, Space, Select, Button, message, Divider, Popconfirm, Row, Tooltip } from 'antd';
import { MinusCircleOutlined, PlusOutlined, AlertOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { CommonFields } from './CommonFields';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';
import { GetMyToken } from '../../entity/dataset/whoAmI';
import { WhereAmI } from '../../home/whereAmI';
// import { DataPlatformSelect } from '../../entity/shared/tabs/Dataset/platformSelect/DataPlatformSelect';
import { printErrorMsg, printSuccessMsg } from '../../entity/shared/tabs/Dataset/ApiCallUtils';
import { SetParentContainer } from '../../entity/shared/tabs/Dataset/containerEdit/SetParentContainer';
import { SpecifyBrowsePath } from './SpecifyBrowsePath';

const SearchResultContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 2px;
`;

const SearchResultContent = styled.div`
    display: flex;
    justify-content: start;
    align-items: center;
`;

const SearchResultDisplayName = styled.div`
    margin-left: 5px;
`;

const platformSelection = [
    'urn:li:dataPlatform:csv',
    // 'urn:li:dataPlatform:mongodb',
    // 'urn:li:dataPlatform:elasticsearch',
    // 'urn:li:dataPlatform:mssql',
    'urn:li:dataPlatform:mysql',
    // 'urn:li:dataPlatform:oracle',
    // 'urn:li:dataPlatform:mariadb',
    // 'urn:li:dataPlatform:hdfs',
    'urn:li:dataPlatform:hive',
    'urn:li:dataPlatform:kudu',
    'urn:li:dataPlatform:postgres',
];

export const CsvForm = () => {
    const urlBase = WhereAmI();
    const publishUrl = `${urlBase}custom/make_dataset`;
    const [visible, setVisible] = useState(false);
    const [confirmLoading, setConfirmLoading] = useState(false);
    console.log(`the publish url is ${publishUrl}`);
    const user = useGetAuthenticatedUser();
    const userUrn = user?.corpUser?.urn || '';
    const userToken = GetMyToken(userUrn);
    // const aboutType = 'Specify the datatype for field. If unsure, set "unknown"';
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
        if (data.length > 0 && headerLine <= data.length) {
            const res = data[headerLine].data.map((item) => {
                return { field_name: item, field_description: '' };
            });
            form.setFieldsValue({ fields: res });
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
    const [selectedPlatform, setSelectedPlatform] = useState('urn:li:dataPlatform:csv');
    const renderSearchResult = (result: string) => {
        const displayName = result.split(':').pop()?.toUpperCase();
        return (
            <SearchResultContainer>
                <SearchResultContent>
                    <SearchResultDisplayName>
                        <div>{displayName}</div>
                    </SearchResultDisplayName>
                </SearchResultContent>
            </SearchResultContainer>
        );
    };
    // const aboutPlatform =
    //     'If dataset is not from an existing database, use FILE. For databases not in list, refer to admin';
    const onSelectMember = (urn: string) => {
        setSelectedPlatform(urn);
        form.setFieldsValue({ parentContainer: '' });
        form.setFieldsValue({ browsepathList: [{ browsepath: '' }] });
    };
    console.log(`selected platform: ${selectedPlatform}`);
    const removeOption = () => {
        console.log(`removing ${selectedPlatform}`);
        setSelectedPlatform('');
        form.setFieldsValue({ parentContainer: '' });
    };
    const popupMsg = `Confirm Dataset Name: ${form.getFieldValue('dataset_name')}? 
    This permanently affects the dataset URL`;
    // const { TextArea } = Input;
    return (
        <>
            <Form
                {...layout}
                name="main_form_item"
                form={form}
                initialValues={{
                    dataset_name: 'abc',
                    fields: [{ field_description: '', field_type: 'string', field_name: 'field1' }],
                    browsepathList: [{ browsepath: `/${selectedPlatform.split(':').pop()}/` }],
                    frequency: 'Unknown',
                    dataset_origin: 'dunno',
                    dataset_location: 'dumpster',
                    platformSelect: 'urn:li:dataPlatform:csv',
                }}
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
                <Tooltip title="aBC">
                    <Row>
                        <Col span={2} />
                        <Col span={10}>
                            <Form.Item
                                name="platformSelect"
                                label="Specify a Data Source Type"
                                rules={[
                                    {
                                        required: true,
                                        message: 'A type MUST be specified.',
                                    },
                                ]}
                            >
                                <Select
                                    value={selectedPlatform}
                                    defaultValue=""
                                    showArrow
                                    placeholder="Search for a parent container.."
                                    onSelect={(platform: string) => onSelectMember(platform)}
                                    allowClear
                                    onClear={removeOption}
                                    onDeselect={removeOption}
                                >
                                    {platformSelection?.map((platform) => (
                                        <Select.Option value={platform}>{renderSearchResult(platform)}</Select.Option>
                                    ))}
                                </Select>
                            </Form.Item>
                        </Col>
                    </Row>
                </Tooltip>
                <Row>
                    <Col span={2} />
                    <Col span={10}>
                        <SetParentContainer platformType={selectedPlatform} compulsory={false} />
                    </Col>
                </Row>
                <CommonFields />
                <SpecifyBrowsePath />
                <Form.Item label="Dataset Fields" name="fields">
                    <Form.List name="fields">
                        {(fields, { add, remove }) => (
                            <>
                                {fields.map(({ key, name, fieldKey, ...restField }) => (
                                    <Space key={key}>
                                        <Form.Item
                                            {...restField}
                                            name={[name, 'field_name']}
                                            fieldKey={[fieldKey, 'field_name']}
                                            rules={[{ required: true, message: 'Missing field name' }]}
                                        >
                                            <Input placeholder="Field Name" />
                                        </Form.Item>
                                        <Tooltip title="fIELD tYPE">
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
                                    </Space>
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
