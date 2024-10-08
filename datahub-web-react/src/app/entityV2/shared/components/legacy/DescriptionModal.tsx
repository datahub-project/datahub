import { Typography, Modal, Button, Form } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { Editor } from '../../tabs/Documentation/components/editor/Editor';
import { ANTD_GRAY } from '../../constants';
import InferDocsPanel from '../inferredDocs/InferDocsPanel';
import { useMutationUrn } from '../../../../entity/shared/EntityContext';

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
}: Props) {
    const urn = useMutationUrn();
    const [updatedDesc, setDesc] = useState(description || original || '');
    const [editorKey, setEditorKey] = useState(0);

    return (
        <Modal
            title={title}
            open
            width={900}
            onCancel={onClose}
            okText={isAddDesc ? 'Submit' : 'Update'}
            footer={
                <>
                    <Button onClick={onClose}>Cancel</Button>
                    {showPropose && onPropose && (
                        <Button onClick={() => onPropose(updatedDesc)} disabled={updatedDesc === description}>
                            Propose
                        </Button>
                    )}
                    <Button
                        onClick={() => onSubmit(updatedDesc)}
                        disabled={updatedDesc === description}
                        data-testid="description-modal-update-button"
                    >
                        Publish
                    </Button>
                </>
            }
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
                    <StyledEditor key={editorKey} content={updatedDesc} onChange={setDesc} />
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
