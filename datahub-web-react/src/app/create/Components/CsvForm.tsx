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
import { printErrorMsg, printSuccessMsg } from '../../entity/shared/tabs/Dataset/ApiCallUtils';
import { SpecifyBrowsePath } from './SpecifyBrowsePath';
import { SetParentContainer } from '../../entity/shared/tabs/Dataset/containerEdit/SetParentContainer';

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
    'urn:li:dataPlatform:file',
    'urn:li:dataPlatform:mongodb',
    'urn:li:dataPlatform:elasticsearch',
    'urn:li:dataPlatform:mssql',
    'urn:li:dataPlatform:mysql',
    'urn:li:dataPlatform:oracle',
    'urn:li:dataPlatform:mariadb',
    'urn:li:dataPlatform:hdfs',
    'urn:li:dataPlatform:hive',
    'urn:li:dataPlatform:kudu',
    'urn:li:dataPlatform:postgres',
];

export const CsvForm = () => {
    const urlBase = WhereAmI();
    const publishUrl = `${urlBase}custom/make_dataset`;
    const [visible, setVisible] = useState(false);
    const [confirmLoading, setConfirmLoading] = useState(false);
    const user = useGetAuthenticatedUser();
    const userUrn = user?.corpUser?.urn || '';
    const userToken = GetMyToken(userUrn);
    const sourceType = 'Select a Datasouce Type. For sources not listed, refer to admin team';

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
        const finalValue = { ...values, dataset_owner: user?.corpUser?.username, user_token: userToken };
        delete finalValue.parentContainerProps;
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
    const handleOnFileLoad = (data) => {
        const headerLine = 0;
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
    const [selectedPlatform, setSelectedPlatform] = useState('urn:li:dataPlatform:file');

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
    //     'If dataset is not from an existing database, use FILE. For databases not in list, refer to admin';
    const onSelectMember = (urn: string) => {
        setSelectedPlatform(urn);
        form.setFieldsValue({ parentContainerProps: { platformType: urn, platformContainer: '' } });
        form.setFieldsValue({ parentContainer: '' });
    };
    // const removeOption = () => {
    //     setSelectedPlatform('');
    //     //  todo why do we need this line?
    //     form.setFieldsValue({ parentContainer: '' });
    // };
    const popupMsg = `Confirm Dataset Name: ${form.getFieldValue('dataset_name')}? 
    This permanently affects the dataset URL`;
    const { TextArea } = Input;
    const formIsUpdating = () => {};
    // todo: remove similar variables like parentContainer and platformSelect since we have parentContainerProps object
    return (
        <>
            <Form
                {...layout}
                name="main_form_item"
                form={form}
                initialValues={{
                    dataset_name: '',
                    fields: [{ field_description: '', field_type: 'string', field_name: '' }],
                    browsepathList: [{ browsepath: `/STAGING/` }],
                    frequency: 'Unknown',
                    dataset_frequency_details: '',
                    dataset_origin: '',
                    dataset_location: '',
                    platformSelect: 'urn:li:dataPlatform:file',
                    parentContainerProps: {
                        platformType: 'urn:li:dataPlatform:file',
                        platformContainer: '',
                    },
                }}
                onFieldsChange={formIsUpdating}
            >
                <Row>
                    <Col span={4} />
                    <Col span={16}>
                        <CSVReader onFileLoad={handleOnFileLoad} addRemoveButton onRemoveFile={handleOnRemoveFile}>
                            <span>
                                Click here to pre-generate dictionary from your data file header (CSV or delimited text
                                file only)
                            </span>
                        </CSVReader>
                    </Col>
                </Row>
                <Divider dashed orientation="left">
                    Dataset Info
                </Divider>
                <Tooltip title={sourceType}>
                    <Form.Item
                        name="platformSelect"
                        label="Specify a Data Source Type."
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
                            placeholder="Search for a type.."
                            onSelect={(platform: string) => onSelectMember(platform)}
                            style={{ width: '20%' }}
                        >
                            {platformSelection?.map((platform) => (
                                <Select.Option value={platform}>{renderSearchResult(platform)}</Select.Option>
                            ))}
                        </Select>
                    </Form.Item>
                </Tooltip>
                <Form.Item
                    label="Specify a Container(Optional)"
                    rules={[
                        {
                            required: false,
                            message: 'A container must be specified.',
                        },
                    ]}
                    style={{ marginBottom: '0px' }}
                >
                    <Form.Item name="parentContainerProps">
                        <SetParentContainer />
                    </Form.Item>
                </Form.Item>
                <CommonFields />
                <SpecifyBrowsePath />
                <Form.Item label="Dataset Fields" name="fields">
                    <Form.List name="fields">
                        {(fields, { add, remove }) => (
                            <>
                                {fields.map(({ key, name, ...restField }) => (
                                    <Row key={key}>
                                        <Col span={6}>
                                            <Form.Item
                                                {...restField}
                                                name={[name, 'field_name']}
                                                rules={[{ required: true, message: 'Missing field name' }]}
                                            >
                                                <Input placeholder="Field Name" />
                                            </Form.Item>
                                        </Col>
                                        &nbsp;
                                        <Tooltip
                                            title="Select column type. Unknown type is also available if unsure"
                                            placement="left"
                                        >
                                            <Col span={3}>
                                                <Form.Item
                                                    {...restField}
                                                    name={[name, 'field_type']}
                                                    rules={[{ required: true, message: 'Missing field type' }]}
                                                >
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
                                                </Form.Item>
                                            </Col>
                                        </Tooltip>
                                        &nbsp;
                                        <Col span={11}>
                                            <Form.Item
                                                {...restField}
                                                name={[name, 'field_description']}
                                                rules={[
                                                    {
                                                        required: false,
                                                        message: 'Missing field description',
                                                    },
                                                ]}
                                            >
                                                <TextArea
                                                    rows={4}
                                                    placeholder="Field Description"
                                                    autoSize={{ minRows: 1, maxRows: 3 }}
                                                />
                                            </Form.Item>
                                        </Col>
                                        {fields.length > 1 ? (
                                            <Col span={1}>
                                                <Form.Item>
                                                    <Button>
                                                        <MinusCircleOutlined
                                                            data-testid="delete-icon"
                                                            onClick={() => remove(name)}
                                                        />
                                                    </Button>
                                                </Form.Item>
                                            </Col>
                                        ) : null}
                                    </Row>
                                ))}
                                <Row>
                                    <Col span={21} offset={0}>
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
