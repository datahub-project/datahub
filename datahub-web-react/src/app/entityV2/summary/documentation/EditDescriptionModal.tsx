import { Editor, Modal } from '@components';
import { message } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useDocumentationPermission } from '@app/entityV2/summary/documentation/useDocumentationPermission';
import useFileUpload from '@app/shared/hooks/useFileUpload';

import { UploadDownloadScenario } from '@types';

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
    const { urn: assetUrn } = useEntityData();
    const { uploadFile } = useFileUpload({ scenario: UploadDownloadScenario.AssetDocumentation, assetUrn });
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
                        handleDescriptionUpdate().catch((e) => {
                            message.destroy();
                            message.error({
                                content: `Failed to update description: \n ${e.message || ''}`,
                                duration: 3,
                            });
                        });
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
                uploadFile={uploadFile}
            />
        </Modal>
    );
}
