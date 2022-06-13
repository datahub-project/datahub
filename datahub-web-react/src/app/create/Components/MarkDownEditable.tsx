import React from 'react';
import { Col, Form, Row } from 'antd';
import MDEditor, { commands } from '@uiw/react-md-editor';
import { TemplateDescriptionString } from '../../../conf/Adhoc';

export const MarkDownEditable = () => {
    // I don't want the fullscreen option, hence need to specify the commands.
    const previewIcons = [commands.codeEdit, commands.codePreview, commands.codeLive];
    return (
        <>
            <Form.Item
                name="dataset_description"
                label="Information about Dataset"
                rules={[
                    {
                        required: false,
                        message: 'Missing dataset description',
                    },
                ]}
            >
                <Row>
                    <Col span={20} offset={0}>
                        <MDEditor
                            // placeholder={TemplateDescriptionString}
                            // value={TemplateDescriptionString}
                            textareaProps={{
                                placeholder: TemplateDescriptionString,
                            }}
                            preview="live"
                            extraCommands={previewIcons}
                            enableScroll={false}
                            style={{ border: '1px solid white' }}
                        />
                    </Col>
                </Row>
            </Form.Item>
        </>
    );
};
