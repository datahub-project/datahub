import { Editor, Modal } from '@components';
import { Form, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { EditorProps } from '@components/components/Editor/types';
import { ModalButton } from '@components/components/Modal/Modal';

import { useMutationUrn } from '@app/entity/shared/EntityContext';
import InferDocsPanel from '@app/entityV2/shared/components/inferredDocs/InferDocsPanel';
import { StyledEditor } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/note/AssertionNoteTab';

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

type Props = {
    title: string;
    fieldPath?: string;
    description?: string;
    original?: string;
    propagatedDescription?: string;
    inferredDescription?: string;
    onClose: () => void;
    onSubmit: (description: string) => void;
    onPropose?: (description: string) => void;
    isAddDesc?: boolean;
    showPropose?: boolean;
    inferOnMount?: boolean;
    isEmbeddedProfile?: boolean;
    canEditDescription?: boolean;
    canProposeDescription?: boolean;
    editorProps?: Partial<EditorProps>;
};

export default function UpdateDescriptionModal({
    title,
    description,
    fieldPath,
    original,
    propagatedDescription,
    inferredDescription,
    onClose,
    onSubmit,
    onPropose,
    isAddDesc,
    showPropose,
    inferOnMount,
    isEmbeddedProfile,
    canEditDescription = true,
    canProposeDescription = true,
    editorProps,
}: Props) {
    const urn = useMutationUrn();
    const [updatedDesc, setDesc] = useState(description || original || '');
    const [editorKey, setEditorKey] = useState(0);

    const buttons: ModalButton[] = [
        {
            text: 'Cancel',
            variant: 'text',
            onClick: onClose,
        },
        ...(showPropose && onPropose
            ? [
                  {
                      text: 'Propose',
                      variant: 'outline' as const,
                      onClick: () => onPropose(updatedDesc),
                      disabled: updatedDesc === description || !canProposeDescription,
                  },
              ]
            : []),
        {
            text: 'Publish',
            onClick: () => onSubmit(updatedDesc),
            variant: 'filled',
            disabled: updatedDesc === description || !canEditDescription,
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
                {!isAddDesc && description && inferredDescription && (
                    <OriginalDocumentation label={<FormLabel>AI Generated:</FormLabel>}>
                        <StyledViewer content={inferredDescription || ''} readOnly />
                    </OriginalDocumentation>
                )}
                <Form.Item>
                    <StyledEditor key={editorKey} content={updatedDesc} onChange={setDesc} {...editorProps} />
                </Form.Item>

                {(fieldPath || isEmbeddedProfile) && (
                    <InferDocsPanel
                        urn={urn}
                        forColumnPath={fieldPath}
                        inferOnMount={inferOnMount}
                        onInsertDescription={(desc) => {
                            setDesc(updatedDesc + desc);
                            setEditorKey((key) => key + 1);
                        }}
                        surface="schema-docs-editor"
                    />
                )}
            </Form>
        </Modal>
    );
}
