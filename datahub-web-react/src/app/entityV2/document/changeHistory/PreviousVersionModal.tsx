import { Modal, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { Button, Editor } from '@src/alchemy-components';

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
            color: ${(props) => props.theme.colors.textSecondary};
        }
        p:last-of-type {
            margin-bottom: 0;
        }
    }
`;

const EmptyContent = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    font-style: italic;
    padding: 20px;
`;

const ModalFooter = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    padding-top: 16px;
    border-top: 1px solid ${(props) => props.theme.colors.border};
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
    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
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

            message.success(t('document.documentRestoredSuccess'));

            // Close modals
            setShowConfirmRestore(false);
            onClose();
        } catch (error) {
            console.error('Failed to restore document content:', error);
            message.error(t('document.documentRestoreError'));
        }
    };

    return (
        <>
            <Modal
                title={t('document.previousVersionTitle')}
                open={open}
                onCancel={onClose}
                width={1200}
                footer={null}
                style={{ top: 40 }}
            >
                <ModalContent>
                    <ContentPreview>
                        {previousContent ? (
                            <StyledEditor
                                content={previousContent}
                                readOnly
                                placeholder={t('document.noContentPlaceholder')}
                                hideBorder
                            />
                        ) : (
                            <EmptyContent>{t('document.emptyContent')}</EmptyContent>
                        )}
                    </ContentPreview>

                    <ModalFooter>
                        <Button variant="text" color="gray" onClick={onClose} disabled={restoring}>
                            {tc('cancel')}
                        </Button>
                        <Button variant="filled" onClick={() => setShowConfirmRestore(true)} disabled={restoring}>
                            {tc('restore')}
                        </Button>
                    </ModalFooter>
                </ModalContent>
            </Modal>

            <ConfirmationModal
                isOpen={showConfirmRestore}
                handleClose={() => setShowConfirmRestore(false)}
                handleConfirm={handleRestore}
                modalTitle={t('document.restoreVersionConfirmation')}
                modalText={t('document.restoreVersionConfirmationBody')}
                confirmButtonText={tc('restore')}
            />
        </>
    );
};
