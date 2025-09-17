import { Form, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ModalButton } from '@components/components/Modal/Modal';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { Editor } from '@app/entityV2/shared/tabs/Documentation/components/editor/Editor';
import { Modal } from '@src/alchemy-components';

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

    const buttons: ModalButton[] = [
        {
            text: 'Cancel',
            variant: 'text',
            onClick: onClose,
        },
        {
            text: 'Publish',
            onClick: () => onSubmit(updatedDesc),
            variant: 'filled',
            disabled: updatedDesc === description,
            buttonDataTestId: 'description-modal-update-button',
        },
    ];

    return (
        <Modal
            title={title}
            open
            width={900}
            onCancel={onClose}
            okText={isAddDesc ? 'Submit' : 'Update'}
            buttons={buttons}
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
                    <StyledEditor content={updatedDesc} onChange={setDesc} />
                </Form.Item>
            </Form>
        </Modal>
    );
}
