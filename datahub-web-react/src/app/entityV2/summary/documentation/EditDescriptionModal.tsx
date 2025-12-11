/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Editor, Modal } from '@components';
import { message } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useDocumentationPermission } from '@app/entityV2/summary/documentation/useDocumentationPermission';
import useFileUpload from '@app/shared/hooks/useFileUpload';
import useFileUploadAnalyticsCallbacks from '@app/shared/hooks/useFileUploadAnalyticsCallbacks';

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
    const uploadFileAnalyticsCallbacks = useFileUploadAnalyticsCallbacks({
        scenario: UploadDownloadScenario.AssetDocumentation,
        assetUrn,
    });
    const { uploadFile } = useFileUpload({ scenario: UploadDownloadScenario.AssetDocumentation, assetUrn });
    return (
        <Modal
            title="Edit Description"
            onCancel={closeModal}
            width="80vw"
            style={{ maxWidth: '1200px' }}
            maskClosable={false}
            buttons={[
                {
                    text: 'Cancel',
                    variant: 'text',
                    onClick: () => closeModal(),
                    buttonDataTestId: 'cancel-button',
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
                    buttonDataTestId: 'publish-button',
                },
            ]}
        >
            <StyledEditor
                content={updatedDescription}
                placeholder={emptyDescriptionText}
                hideHighlightToolbar
                onChange={(description) => setUpdatedDescription(description)}
                toolbarStyles={toolbarStyles}
                dataTestId="description-editor"
                uploadFileProps={{
                    onFileUpload: uploadFile,
                    ...uploadFileAnalyticsCallbacks,
                }}
            />
        </Modal>
    );
}
