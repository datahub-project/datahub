import { Editor, Modal } from '@components';
import { Form, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

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

const OriginalDocumentation = styled(Form.Item)`
    margin-bottom: 12px;
`;

type Props = {
    title: string;
    description?: string;
    original?: string;
    propagatedDescription?: string;
    onClose: () => void;
    onSubmit: (description: string) => void;
    isAddDesc?: boolean;
};

export default function UpdateDescriptionModal({
    title,
    description,
    original,
    propagatedDescription,
    onClose,
    onSubmit,
    isAddDesc,
}: Props) {
    const [updatedDesc, setDesc] = useState(description || original || '');

    return (
        <Modal
            title={title}
            open
            width={900}
            onCancel={onClose}
            okText={isAddDesc ? 'Submit' : 'Update'}
            buttons={[
                { text: 'Cancel', variant: 'outline', onClick: onClose },
                {
                    text: 'Publish',
                    variant: 'filled',
                    onClick: () => onSubmit(updatedDesc),
                    disabled: updatedDesc === description,
                    buttonDataTestId: 'description-modal-update-button',
                },
            ]}
        >
            <Form layout="vertical">
                {!isAddDesc && description && original && (
                    <OriginalDocumentation label={<FormLabel>Original:</FormLabel>}>
                        <StyledViewer content={original || ''} readOnly />
                    </OriginalDocumentation>
                )}
                {!isAddDesc && description && propagatedDescription && (
                    <OriginalDocumentation label={<FormLabel>Propagated:</FormLabel>}>
                        <StyledViewer content={propagatedDescription || ''} readOnly />
                    </OriginalDocumentation>
                )}
                <Form.Item>
                    <StyledEditor content={updatedDesc} onChange={setDesc} dataTestId="description-editor" />
                </Form.Item>
            </Form>
        </Modal>
    );
}
