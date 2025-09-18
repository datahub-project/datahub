import { Editor, Modal } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useDocumentationPermission } from '@app/entityV2/summary/documentation/useDocumentationPermission';

const StyledEditor = styled(Editor)`
    border: none;
    &&& {
        .remirror-editor {
            padding: 0;
            min-height: 350px;
            max-height: calc(100vh - 300px);
            overflow: auto;
        }
    }
`;

const toolbarStyles = {
    marginLeft: '-8px',
    marginBottom: '16px',
    width: '100%',
    justifyContent: 'flex-start',
    marginTop: '-24px',
};

interface Props {
    updatedDescription: string;
    setUpdatedDescription: React.Dispatch<React.SetStateAction<string>>;
    handleDescriptionUpdate: () => Promise<void>;
    emptyDescriptionText: string;
    closeModal: () => void;
}

export default function EditDescriptionModal({
    updatedDescription,
    setUpdatedDescription,
    handleDescriptionUpdate,
    emptyDescriptionText,
    closeModal,
}: Props) {
    const canEditDescription = useDocumentationPermission();
    return (
        <Modal
            title="Edit Description"
            onCancel={closeModal}
            width="80vw"
            style={{ maxWidth: '1200px' }}
            buttons={[
                {
                    text: 'Cancel',
                    variant: 'text',
                    onClick: () => closeModal(),
                },
                {
                    text: 'Publish',
                    onClick: () => {
                        handleDescriptionUpdate();
                        closeModal();
                    },
                    disabled: !canEditDescription,
                },
            ]}
        >
            <StyledEditor
                content={updatedDescription}
                placeholder={emptyDescriptionText}
                hideHighlightToolbar
                onChange={(description) => setUpdatedDescription(description)}
                toolbarStyles={toolbarStyles}
            />
        </Modal>
    );
}
