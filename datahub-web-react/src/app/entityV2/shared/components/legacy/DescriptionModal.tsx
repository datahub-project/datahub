import { Editor, Modal, colors } from '@components';
import { Form, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { EditorProps } from '@components/components/Editor/types';
import { ModalButton } from '@components/components/Modal/Modal';

const FormLabel = styled(Typography.Text)`
    font-size: 10px;
    font-weight: bold;
`;

const StyledViewer = styled(Editor)`
    .remirror-editor.ProseMirror {
        padding: 0;
    }
`;

const OriginalDocumentation = styled(Form.Item)`
    margin-bottom: 12px;
`;

const EditorContainer = styled.div`
    height: 200px;
    overflow: auto;
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px;
`;

type Props = {
    title: string;
    description?: string;
    original?: string;
    propagatedDescription?: string;
    onClose: () => void;
    onSubmit: (description: string) => void;
    isAddDesc?: boolean;
    editorProps?: Partial<EditorProps>;
};

export default function UpdateDescriptionModal({
    title,
    description,
    original,
    propagatedDescription,
    onClose,
    onSubmit,
    isAddDesc,
    editorProps,
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
                    <EditorContainer>
                        <Editor
                            content={updatedDesc}
                            onChange={setDesc}
                            dataTestId="description-editor"
                            hideBorder
                            {...editorProps}
                        />
                    </EditorContainer>
                </Form.Item>
            </Form>
        </Modal>
    );
}
