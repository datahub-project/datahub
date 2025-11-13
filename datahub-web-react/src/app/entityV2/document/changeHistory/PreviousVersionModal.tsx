import { Modal, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { Button, Editor } from '@src/alchemy-components';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { useUpdateDocumentContentsMutation } from '@graphql/document.generated';

const ModalContent = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
    max-height: 75vh;
    overflow-y: auto;
`;

const ContentPreview = styled.div`
    padding: 20px;
    border-radius: 8px;
    height: 70vh;
    overflow-y: auto;
`;

const StyledEditor = styled(Editor)`
    border: none;
    &&& {
        .remirror-editor {
            padding: 0px 0;
            min-height: 400px;
        }
        .remirror-editor.ProseMirror {
            font-size: 15px;
            line-height: 1.7;
            color: ${colors.gray[1700]};
        }
        p:last-of-type {
            margin-bottom: 0;
        }
    }
`;

const EmptyContent = styled.div`
    color: ${colors.gray[500]};
    font-style: italic;
    padding: 20px;
`;

const ModalFooter = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    padding-top: 16px;
    border-top: 1px solid #f0f0f0;
`;

interface PreviousVersionModalProps {
    open: boolean;
    onClose: () => void;
    previousContent: string;
    documentUrn: string;
}

export const PreviousVersionModal: React.FC<PreviousVersionModalProps> = ({
    open,
    onClose,
    previousContent,
    documentUrn,
}) => {
    const [updateDocumentContents, { loading: restoring }] = useUpdateDocumentContentsMutation();
    const [showConfirmRestore, setShowConfirmRestore] = useState(false);

    const handleRestore = async () => {
        try {
            // Update the document content
            await updateDocumentContents({
                variables: {
                    input: {
                        urn: documentUrn,
                        contents: {
                            text: previousContent,
                        },
                    },
                },
                refetchQueries: ['getDocument'],
                awaitRefetchQueries: true,
            });

            message.success('Document restored!');

            // Close modals
            setShowConfirmRestore(false);
            onClose();
        } catch (error) {
            console.error('Failed to restore document content:', error);
            message.error('Failed to restore document content');
        }
    };

    return (
        <>
            <Modal
                title="Previous Version"
                open={open}
                onCancel={onClose}
                width={1200}
                footer={null}
                style={{ top: 40 }}
            >
                <ModalContent>
                    <ContentPreview>
                        {previousContent ? (
                            <StyledEditor content={previousContent} readOnly placeholder="No content" hideBorder />
                        ) : (
                            <EmptyContent>(Empty content)</EmptyContent>
                        )}
                    </ContentPreview>

                    <ModalFooter>
                        <Button variant="text" color="gray" onClick={onClose} disabled={restoring}>
                            Cancel
                        </Button>
                        <Button variant="filled" onClick={() => setShowConfirmRestore(true)} disabled={restoring}>
                            Restore
                        </Button>
                    </ModalFooter>
                </ModalContent>
            </Modal>

            <ConfirmationModal
                isOpen={showConfirmRestore}
                handleClose={() => setShowConfirmRestore(false)}
                handleConfirm={handleRestore}
                modalTitle="Restore this version?"
                modalText="This will replace the current document content with this previous version."
                confirmButtonText="Restore"
            />
        </>
    );
};
