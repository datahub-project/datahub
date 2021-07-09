import React, { useEffect, useState } from 'react';
import JsonSchemaEditor from '@optum/json-schema-editor';
import { JsonPointer } from 'json-ptr';
import { JSONSchema7 } from '@optum/json-schema-editor/dist/JsonSchemaEditor.types';
import { Button, Divider, Form, message, Space } from 'antd';
import Dragger from 'antd/lib/upload/Dragger';
import { v4 as uuidv4 } from 'uuid';
import { CommonFields } from './CommonFields';

export const JsonForm = () => {
    const exampleSchema =
        '{\n' +
        '    "$schema": "http://json-schema.org/draft-07/schema#",\n' +
        '    "type": "object",\n' +
        '    "properties": {\n' +
        '        "fruits": {\n' +
        '            "title": "fruits",\n' +
        '            "type": "array",\n' +
        '            "items": {\n' +
        '                "type": "string"\n' +
        '            }\n' +
        '        },\n' +
        '        "vegetables": {\n' +
        '            "title": "vegetables",\n' +
        '            "type": "array",\n' +
        '            "items": {\n' +
        '                "type": "object",\n' +
        '                "properties": {\n' +
        '                    "veggieName": {\n' +
        '                        "title": "veggieName",\n' +
        '                        "type": "string"\n' +
        '                    },\n' +
        '                    "veggieLike": {\n' +
        '                        "title": "veggieLike",\n' +
        '                        "type": "boolean"\n' +
        '                    }\n' +
        '                },\n' +
        '                "additionalProperties": true\n' +
        '            }\n' +
        '        }\n' +
        '    },\n' +
        '    "additionalProperties": true\n' +
        '}';
    const [state, setState] = useState({ schema: {} });
    const [fields, setFields] = useState({
        fields: [{}],
    });
    const [key, setKey] = useState(uuidv4());
    const [form] = Form.useForm();
    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 16,
        },
    };
    const printIt = (data) => {
        console.log(JSON.parse(data));
    };
    const onFinish = (values) => {
        console.log(values);
    };
    const flattenSchema = (schemaStr) => {
        // use json pointer to get all fields and its parent
        JsonPointer.visit(JSON.parse(schemaStr), (p, v) => {
            const paths = JsonPointer.decode(p);
            const jsonValue = JSON.parse(JSON.stringify(v as string));
            // if path length is 0 (root)
            if (paths.length === 0) {
                setFields({
                    fields: [{ field_name: 'root', field_type: jsonValue.type, field_description: '' }],
                });
            }
            if (paths.length > 0) {
                // contain field information
                if (typeof jsonValue === 'object') {
                    const lineage = new JsonPointer(paths.slice(0, paths.length - 1)).path;
                    const parent = lineage[lineage.length - 1];
                    let finalTitle = '';
                    if (parent === 'properties' && jsonValue.hasOwnProperty('title')) {
                        const titleWithPrefix = lineage
                            .filter((x) => (x as string) !== 'properties' && (x as string) !== 'items')
                            .join('.');
                        if (titleWithPrefix === '') {
                            finalTitle = 'root.'.concat(jsonValue.title);
                        } else {
                            finalTitle = 'root.'.concat(titleWithPrefix, '.', jsonValue.title);
                        }
                        setFields((prevState) => ({
                            fields: [
                                ...prevState.fields,
                                {
                                    field_name: finalTitle,
                                    field_type: jsonValue.type,
                                    field_description: jsonValue.description,
                                },
                            ],
                        }));
                    }
                }
            }
        });
    };
    const onReset = () => {
        form.resetFields();
        setState({ schema: {} });
        setKey(uuidv4());
        flattenSchema(exampleSchema);
    };
    useEffect(() => {
        console.log('Do something after counter has changed', fields);
    }, [fields]);
    const props = {
        name: 'file',
        maxCount: 1,
        multiple: false,
        action: 'http://localhost:9002/jsonSchema',
        accept: 'application/json',
        onChange(info) {
            const { status } = info.file;
            if (status === 'done') {
                console.log('info:', info.file.response);
                const newSchema: JSONSchema7 = info.file.response;
                setState({ schema: newSchema });
                setKey(uuidv4());
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
                    initialValues={{ fields: [{ field_description: '' }] }}
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
