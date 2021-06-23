import React, { useState } from 'react';
import axios from 'axios';
import { CSVReader } from 'react-papaparse';
import { Form, Input, Space, Select, Button, message, Layout, Breadcrumb, Menu, Typography, Divider } from 'antd';
import styled from 'styled-components';
import { Content } from 'antd/lib/layout/layout';
import Sider from 'antd/lib/layout/Sider';
import SubMenu from 'antd/lib/menu/SubMenu';
import { MinusCircleOutlined, PlusOutlined, SettingFilled } from '@ant-design/icons';
import adhocConfig from '../../conf/Adhoc';
import { SearchablePage } from '../search/SearchablePage';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';

const Title = styled(Typography.Text)`
    && {
        font-size: 32px;
        color: ${(props) => props.theme.styles['homepage-background-upper-fade']};
    }
`;

export const AdHocPage = () => {
    const [fileType, setFileType] = useState({ dataset_type: 'application/octet-stream' });
    const [form] = Form.useForm();
    const { Option } = Select;
    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 12,
        },
    };
    const user = useGetAuthenticatedUser();
    const printSuccessMsg = (status) => {
        message.success(`Status:${status} - Request submitted successfully`, 3).then();
    };
    const printErrorMsg = (error) => {
        message.error(error, 3).then();
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
                return { field_name: item };
            });
            form.setFieldsValue({ fields: res });
        }
    };
    const handleOnRemoveFile = () => {
        form.setFieldsValue({ fields: [{}] });
    };
    return (
        <>
            <SearchablePage>
                <Layout>
                    <Content style={{ padding: '0 50px' }}>
                        <Breadcrumb style={{ margin: '16px 0' }}>
                            <Breadcrumb.Item>Home</Breadcrumb.Item>
                            <Breadcrumb.Item>Adhoc</Breadcrumb.Item>
                        </Breadcrumb>
                        <Layout className="site-layout-background" style={{ padding: '24px 0' }}>
                            <Sider className="site-layout-background" width={200}>
                                <Menu
                                    mode="inline"
                                    defaultSelectedKeys={['1']}
                                    defaultOpenKeys={['sub1']}
                                    style={{ height: '100%' }}
                                >
                                    <SubMenu key="sub1" icon={<SettingFilled spin />} title="Adhoc Dataset">
                                        <Menu.Item key="1">Create</Menu.Item>
                                        <Menu.Item key="3">Delete</Menu.Item>
                                        <Menu.Item key="2">List</Menu.Item>
                                    </SubMenu>
                                </Menu>
                            </Sider>
                            <Content style={{ padding: '0 24px', minHeight: 280 }}>
                                <Title>
                                    <b>Create </b>
                                    your own dataset
                                </Title>
                                <br />
                                <br />
                                <Form
                                    {...layout}
                                    form={form}
                                    initialValues={{ fields: [{}] }}
                                    name="dynamic_form_item"
                                    onFinish={onFinish}
                                >
                                    <CSVReader
                                        onFileLoad={handleOnFileLoad}
                                        addRemoveButton
                                        onRemoveFile={handleOnRemoveFile}
                                    >
                                        <span>
                                            Click here to parse your file header (CSV or delimited text file only)
                                        </span>
                                    </CSVReader>
                                    <Divider dashed orientation="left">
                                        Dataset Info
                                    </Divider>
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
                                        <Input />
                                    </Form.Item>
                                    <Form.Item
                                        name="dataset_description"
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
                                        <Input />
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
                                        <Input />
                                    </Form.Item>
                                    <Form.Item label="Dataset Fields" name="dataset_fields">
                                        <Form.List {...layout} name="fields">
                                            {(fields, { add, remove }) => (
                                                <>
                                                    {fields.map(({ key, name, fieldKey, ...restField }) => (
                                                        <Space
                                                            key={key}
                                                            style={{ display: 'flex', marginBottom: 8 }}
                                                            align="baseline"
                                                        >
                                                            <Form.Item
                                                                {...restField}
                                                                name={[name, 'field_name']}
                                                                fieldKey={[fieldKey, 'field_name']}
                                                                rules={[
                                                                    { required: true, message: 'Missing field name' },
                                                                ]}
                                                            >
                                                                <Input placeholder="Field Name" />
                                                            </Form.Item>
                                                            <Form.Item
                                                                {...restField}
                                                                name={[name, 'field_type']}
                                                                fieldKey={[fieldKey, 'field_type']}
                                                                rules={[
                                                                    { required: true, message: 'Missing field type' },
                                                                ]}
                                                            >
                                                                <Select
                                                                    showSearch
                                                                    style={{ width: 200 }}
                                                                    placeholder="Select field type"
                                                                    optionFilterProp="children"
                                                                    filterOption={(input, option) =>
                                                                        option?.children
                                                                            .toLowerCase()
                                                                            .indexOf(input.toLowerCase()) >= 0
                                                                    }
                                                                >
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
                                                                <Input placeholder="Field Description" />
                                                            </Form.Item>
                                                            <MinusCircleOutlined onClick={() => remove(name)} />
                                                        </Space>
                                                    ))}
                                                    <Form.Item>
                                                        <Button
                                                            type="dashed"
                                                            onClick={() => add()}
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
                            </Content>
                        </Layout>
                    </Content>
                </Layout>
            </SearchablePage>
        </>
    );
};
