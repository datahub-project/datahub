import React, { useState } from 'react';
import { Form, Input } from 'antd';
import styled from 'styled-components';
import MDEditor, { commands } from '@uiw/react-md-editor';
import { SpecifyBrowsePath } from './SpecifyBrowsePath';

export const CommonFields = () => {
    const DocumentationContainer = styled.div`
        border: 1px solid white;
    `;
    const tempString = `About Dataset:  \nUpdate Frequency:  \nFor Data Access, Contact:  \n`;
    // I don't want the fullscreen option, hence need to specify the commands.
    const previewIcons = [commands.codeLive, commands.codeEdit, commands.codePreview];
    const [descriptionValue, setDescriptionValue] = useState(tempString);
    const updateValue = (values) => {
        setDescriptionValue(values);
    };
    return (
        <>
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
                <DocumentationContainer>
                    <MDEditor
                        value={descriptionValue}
                        onChange={updateValue}
                        preview="live"
                        extraCommands={previewIcons}
                        enableScroll={false}
                    />
                </DocumentationContainer>
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
            <SpecifyBrowsePath />
        </>
    );
};
