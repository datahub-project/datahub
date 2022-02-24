import { Typography, Modal, Button, Form } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import MDEditor from '@uiw/react-md-editor';

const DescriptionMarkdown = styled(MDEditor.Markdown)`
    padding: 4px 10px;
`;

const FormLabel = styled(Typography.Text)`
    font-size: 10px;
    font-weight: bold;
`;

const MarkDownHelpLink = styled(Typography.Link)`
    position: absolute;
    right: 0;
    top: -18px;
    font-size: 12px;
`;

type Props = {
    title: string;
    description?: string | undefined;
    original?: string | undefined;
    onClose: () => void;
    onSubmit: (description: string) => void;
    isAddDesc?: boolean;
};

export default function UpdateDescriptionModal({ title, description, original, onClose, onSubmit, isAddDesc }: Props) {
    const [updatedDesc, setDesc] = useState(description || original || '');

    return (
        <Modal
            title={title}
            visible
            width={900}
            onCancel={onClose}
            okText={isAddDesc ? 'Submit' : 'Update'}
            footer={
                <>
                    <Button onClick={onClose}>Cancel</Button>
                    <Button onClick={() => onSubmit(updatedDesc)} disabled={updatedDesc === description}>
                        Update
                    </Button>
                </>
            }
        >
            <Form layout="vertical">
                {isAddDesc ? (
                    <Form.Item>
                        <MarkDownHelpLink href="https://joplinapp.org/markdown" target="_blank" type="secondary">
                            markdown supported
                        </MarkDownHelpLink>
                        <MDEditor
                            style={{ fontWeight: 400 }}
                            value={updatedDesc}
                            onChange={(v) => setDesc(v || '')}
                            preview="live"
                            height={400}
                        />
                    </Form.Item>
                ) : (
                    <Form.Item>
                        <MarkDownHelpLink href="https://joplinapp.org/markdown" target="_blank" type="secondary">
                            markdown supported
                        </MarkDownHelpLink>
                        <MDEditor
                            style={{ fontWeight: 400 }}
                            value={updatedDesc}
                            onChange={(v) => setDesc(v || '')}
                            preview="live"
                            height={400}
                        />
                    </Form.Item>
                )}
                {!isAddDesc && description && original && (
                    <Form.Item label={<FormLabel>Original:</FormLabel>}>
                        <DescriptionMarkdown source={original || ''} />
                    </Form.Item>
                )}
            </Form>
        </Modal>
    );
}
