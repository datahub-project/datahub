import React, { useEffect, useState } from 'react';
import JsonSchemaEditor from '@optum/json-schema-editor';
import { JsonPointer } from 'json-ptr';
import { JSONSchema7 } from '@optum/json-schema-editor/dist/JsonSchemaEditor.types';
import { Button, Divider, Form, message, Space } from 'antd';
import Dragger from 'antd/lib/upload/Dragger';
import { v4 as uuidv4 } from 'uuid';
import axios from 'axios';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';
import { CommonFields } from './CommonFields';
import { GetMyToken } from '../../entity/dataset/whoAmI';

export const JsonForm = () => {
    // this wacky setup is because the URL is different when running docker-compose vs Ingress
    // for docker-compose, need to change port. For ingress, just modify subpath will do.
    // having a setup that works for both makes development easier.
    // for make_dataset, the URL is simple.
    const initialUrl = window.location.href;
    let publishUrl = initialUrl.includes(':3000') ? initialUrl.replace(':3000/adhoc/', ':8001/custom/make_dataset') : initialUrl;
    publishUrl = publishUrl.includes(':9002') 
        ? publishUrl.replace(':9002/adhoc/', ':8001/custom/make_dataset') 
        : publishUrl.replace('/adhoc/', '/custom/make_dataset');
    const user = useGetAuthenticatedUser();
    const userUrn = user?.corpUser?.urn || '';
    const userToken = GetMyToken(userUrn);
    // console.log(`user is ${userUrn} and token is ${userToken}, received at ${Date().toLocaleString()}`);
    const [state, setState] = useState({
        jsonSchema: {},
        key: '',
    });
    const [schema, setSchema] = useState('');
    const [form] = Form.useForm();
    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 16,
        },
    };
    const printSuccessMsg = (status) => {
        message.success(`Status:${status} - Request submitted successfully`, 3).then();
    };
    const printErrorMsg = (error) => {
        message.error(error, 3).then();
    };
    const onSchemaChange = (data) => {
        setSchema(data);
    };
    const flattenSchema = (schemaStr) => {
        const fields: Array<{ field_name: string; field_type: string; field_description: string }> = [];
        // use json pointer to get all fields and its parent
        JsonPointer.visit(JSON.parse(schemaStr), (p, v) => {
            const jsonValue = JSON.parse(JSON.stringify(v as string));
            if (jsonValue.hasOwnProperty('type')) {
                const paths = JsonPointer.decode(p);
                // if path length is 0 (root)
                /*
                if (paths.length === 0) {
                    fields.push({
                        field_name: 'root',
                        field_type: jsonValue.type,
                        field_description: jsonValue.description,
                    });
                }
                */
                if (paths.length > 0) {
                    // contain field information
                    if (typeof jsonValue === 'object') {
                        const lineage = new JsonPointer(paths.slice(0, paths.length - 1)).path;
                        const parent = lineage[lineage.length - 1];

                        let fieldName = '';
                        if (parent === 'properties') {
                            // use of reduce() method to check for previous item 'properties'
                            lineage.reduce((previous, current) => {
                                const previousItem = previous as string;
                                const currentItem = current as string;
                                if (previousItem === 'properties') {
                                    if (fieldName === '') {
                                        fieldName = fieldName.concat(currentItem);
                                    } else {
                                        fieldName = fieldName.concat('.', currentItem);
                                    }
                                }
                                return current;
                            });

                            // construct title
                            if (fieldName === '') {
                                fieldName = fieldName.concat(jsonValue.title);
                            } else {
                                fieldName = fieldName.concat('.', jsonValue.title);
                            }
                            fields.push({
                                field_name: fieldName,
                                field_type: jsonValue.type,
                                field_description: jsonValue.description,
                            });
                        }
                    }
                }
            }
        });
        return fields;
    };
    const onFinish = (values) => {
        console.log(schema);
        const flattenFields = flattenSchema(schema);
        const data = {
            ...values,
            fields: flattenFields,
            dataset_owner: user?.corpUser?.username,
            dataset_type: 'json',
            user_token: userToken,
        };
        // console.log('Received data:', data);
        // POST request using axios with error handling
        axios
            .post(publishUrl, data)
            .then((response) => printSuccessMsg(response.status))
            .catch((error) => {
                printErrorMsg(error.toString());
            });
    };
    const onReset = () => {
        form.resetFields();
        setState((prev) => ({ ...prev, jsonSchema: {} }));
    };
    useEffect(() => {
        setState((prev) => ({ ...prev, key: uuidv4() }));
    }, [state.jsonSchema]);
    const props = {
        name: 'file',
        maxCount: 1,
        multiple: false,
        action: window.location.origin.concat('/jsonSchema'),
        accept: 'application/json',
        onChange(info) {
            const { status } = info.file;
            if (status === 'done') {
                console.log('info:', info.file.response);
                const newSchema: JSONSchema7 = info.file.response;
                setState((prev) => ({ ...prev, jsonSchema: newSchema }));
                message.success(`${info.file.name} - inferred schema from json file successfully.`).then();
            } else if (status === 'error') {
                message.error(`${info.file.name} - unable to infer schema from json file.`).then();
            }
        },
    };
    // need to set unique key to trigger update the component
    return (
        <div>
            <div>
                <Form
                    {...layout}
                    form={form}
                    initialValues={{
                        fields: [{ field_description: '' }],
                        browsepathList: ['/json/'],
                    }}
                    name="dynamic_form_item"
                    onFinish={onFinish}
                >
                    <Dragger {...props}>
                        <p className="ant-upload-text">Click here to infer schema from json file</p>
                    </Dragger>
                    <Divider dashed orientation="left">
                        Dataset Info
                    </Divider>
                    <CommonFields />
                    <Form.Item label="Dataset Fields" name="fields">
                        <JsonSchemaEditor key={state.key} data={state.jsonSchema} onSchemaChange={onSchemaChange} />
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
            </div>
        </div>
    );
};
