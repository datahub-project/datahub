import { Typography, Modal, Button, Form } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { Editor } from '../../tabs/Documentation/components/editor/Editor';
import { ANTD_GRAY } from '../../constants';

const FormLabel = styled(Typography.Text)`
    font-size: 10px;
    font-weight: bold;
`;

const StyledEditor = styled(Editor)`
    border: 1px solid ${ANTD_GRAY[4.5]};
`;

const StyledViewer = styled(Editor)`
    .remirror-editor.ProseMirror {
        padding: 0;
    }
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
                <Form.Item>
                    <StyledEditor content={updatedDesc} onChange={setDesc} />
                </Form.Item>
                {!isAddDesc && description && original && (
                    <Form.Item label={<FormLabel>Original:</FormLabel>}>
                        <StyledViewer content={original || ''} readOnly />
                    </Form.Item>
                )}
            </Form>
        </Modal>
    );
}
