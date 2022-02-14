import { Typography, Modal, Button, Form } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import MDEditor from '@uiw/react-md-editor';

const MarkDownHelpLink = styled(Typography.Link)`
    position: absolute;
    right: 0;
    top: -18px;
    font-size: 12px;
`;

type Props = {
    title: string;
    description?: string | undefined;
    onClose: () => void;
    onSubmit: (description: string) => void;
    isAddDesc?: boolean;
};

export default function EditDescriptionModal({ title, description, onClose, onSubmit, isAddDesc }: Props) {
    const [updatedDescription, setUpdatedDescription] = useState(description || '');

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
                    <Button onClick={() => onSubmit(updatedDescription)} disabled={updatedDescription === description}>
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
                            value={updatedDescription}
                            onChange={(v) => setUpdatedDescription(v || '')}
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
                            value={updatedDescription}
                            onChange={(v) => setUpdatedDescription(v || '')}
                            preview="live"
                            height={400}
                        />
                    </Form.Item>
                )}
            </Form>
        </Modal>
    );
}
