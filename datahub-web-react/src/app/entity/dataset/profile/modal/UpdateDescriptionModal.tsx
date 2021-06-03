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

type Props = {
    title: string;
    description?: string | undefined;
    original?: string | undefined;
    onClose: () => void;
    onSubmit: (description: string) => void;
    isAddDesc?: boolean;
};

export default function UpdateDescriptionModal({ title, description, original, onClose, onSubmit, isAddDesc }: Props) {
    const [updatedDesc, setDesc] = useState(description);

    return (
        <Modal
            title={title}
            visible
            width={900}
            onCancel={onClose}
            okButtonProps={{ disabled: !updatedDesc || updatedDesc.length === 0 }}
            okText={isAddDesc ? 'Submit' : 'Update'}
            footer={
                <>
                    <Button onClick={onClose}>Cancel</Button>
                    <Button
                        onClick={() => updatedDesc && onSubmit(updatedDesc)}
                        disabled={!updatedDesc || updatedDesc.length === 0 || updatedDesc === description}
                    >
                        Update
                    </Button>
                </>
            }
        >
            <Form layout="vertical">
                {!isAddDesc && (description || original) && (
                    <Form.Item label={<FormLabel>Original:</FormLabel>}>
                        <DescriptionMarkdown source={original || ''} />
                    </Form.Item>
                )}
                {isAddDesc ? (
                    <MDEditor value={updatedDesc} onChange={(v) => setDesc(v || '')} preview="live" height={400} />
                ) : (
                    <Form.Item label={<FormLabel>Updated:</FormLabel>}>
                        <MDEditor value={updatedDesc} onChange={(v) => setDesc(v || '')} preview="live" height={400} />
                    </Form.Item>
                )}
            </Form>
        </Modal>
    );
}
