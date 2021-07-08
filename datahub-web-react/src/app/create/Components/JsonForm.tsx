import React, { useState } from 'react';
import JsonSchemaEditor from '@optum/json-schema-editor';
import { JSONSchema7 } from '@optum/json-schema-editor/dist/JsonSchemaEditor.types';
import { Button, Divider, Form, message, Space } from 'antd';
import Dragger from 'antd/lib/upload/Dragger';
import { v4 as uuidv4 } from 'uuid';
import adhocConfig from '../../../conf/Adhoc';
import { CommonFields } from './CommonFields';

export const JsonForm = () => {
    const jsonSchema: JSONSchema7 = {
        $id: 'https://example.com/person.schema.json',
        title: 'String',
        type: 'object',
        properties: {
            string: {
                description: 'string',
                type: 'string',
            },
        },
    };
    const [state, setState] = useState({ schema: {} });
    const [key, setKey] = useState(uuidv4());
    const [form] = Form.useForm();
    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 14,
        },
    };
    const printIt = (data) => {
        console.log('my data ', data);
    };
    const onFinish = (values) => {
        console.log(values);
    };
    const onReset = () => {
        // todo: remove csv file also
        form.resetFields();
    };
    const props = {
        name: 'file',
        maxCount: 1,
        multiple: false,
        action: adhocConfig,
        accept: 'application/json',
        onChange(info) {
            const { status } = info.file;
            if (status !== 'uploading') {
                console.log(info.file, info.fileList);
            }
            if (status === 'done') {
                message.success(`${info.file.name} file uploaded successfully.`).then();
            } else if (status === 'error') {
                message.error(`${info.file.name} file upload failed.`).then();
                console.log('error');
                setState({ schema: jsonSchema });
                setKey(uuidv4());
            }
        },
        onDrop(e) {
            console.log('Dropped files', e.dataTransfer.files);
        },
    };
    // need to set unique key to trigger update the component
    return (
        <div>
            <div>
                <Form
                    {...layout}
                    form={form}
                    initialValues={{ fields: [{ field_description: '' }] }}
                    name="dynamic_form_item"
                    onFinish={onFinish}
                >
                    <Dragger {...props}>
                        <p className="ant-upload-text">Click here to parse your json file</p>
                    </Dragger>
                    <Divider dashed orientation="left">
                        Dataset Info
                    </Divider>
                    <CommonFields />
                    <Form.Item label="Dataset Fields" name="fields">
                        <JsonSchemaEditor key={key} data={state.schema} onSchemaChange={printIt} />
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
