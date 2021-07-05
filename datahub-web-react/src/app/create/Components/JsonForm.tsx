import React, { useEffect, useState } from 'react';
import JsonSchemaEditor from '@optum/json-schema-editor';
import { JSONSchema7 } from '@optum/json-schema-editor/dist/JsonSchemaEditor.types';
import { Button, Form, Space } from 'antd';
import { CommonFields } from './CommonFields';

interface Props {
    schema: JSONSchema7 | undefined;
}

export const JsonForm = ({ schema }: Props) => {
    const [state, setState] = useState({ schema });
    const [key, setKey] = useState('1');
    const [form] = Form.useForm();
    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 14,
        },
    };
    useEffect(() => {
        const newSchema = schema;
        setState({ schema: newSchema });
    }, [schema]);

    const printIt = (data) => {
        console.log('my data ', data);
    };
    const onFinish = (values) => {
        console.log(values);
    };
    const onReset = () => {
        // todo: remove csv file also
        setState({ schema: undefined });
        setKey('2');
        form.resetFields();
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
