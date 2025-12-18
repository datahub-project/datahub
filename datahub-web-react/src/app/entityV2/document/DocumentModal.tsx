import { LoadingOutlined } from '@ant-design/icons';
import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { DocumentTreeNode } from '@app/document/DocumentTreeContext';
import EntityContext from '@app/entity/shared/EntityContext';
import { DocumentSummaryTab } from '@app/entityV2/document/summary/DocumentSummaryTab';
import { PageTemplateProvider } from '@app/homeV3/context/PageTemplateContext';
import { Modal } from '@src/alchemy-components';

import { useGetDocumentQuery } from '@graphql/document.generated';
import { EntityType, PageTemplateSurfaceType } from '@types';

const StyledModal = styled(Modal)`
    &&& .ant-modal {
        max-height: 95vh;
        top: 2.5vh;
    }

    &&& .ant-modal-content {
        max-height: 95vh;
        height: 95vh;
        position: relative;
        display: flex;
        flex-direction: column;
    }

    .ant-modal-header {
        display: none;
    }

    .ant-modal-body {
        padding: 8px 0 0 0;
        flex: 1 1 auto;
        overflow: hidden;
        display: flex;
        flex-direction: column;
        min-height: 0;
    }

    .ant-modal-footer {
        display: none;
    }

    .ant-modal-close {
        top: 16px;
        right: 16px;
        z-index: 1001;
        width: 24px;
        height: 24px;
        line-height: 1;
        display: flex;
        align-items: center;
        justify-content: center;
        opacity: 0.8;
        transition: opacity 0.2s ease;
        padding: 0;
        border: none;
        background: transparent;

        &:hover {
            opacity: 1;
        }

        .ant-modal-close-x {
            height: 24px;
            width: 24px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 16px;
            color: inherit;
            line-height: 1;
        }
    }
`;

const ModalContent = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    overflow: hidden;
`;

const ContentWrapper = styled.div`
    flex: 1;
    overflow-y: auto;
    padding: 0;
`;

const LoadingWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 200px;
    padding: 40px;
`;

interface DocumentViewModalProps {
    documentUrn: string;
    onClose: () => void;
    onDocumentDeleted?: () => void;
}

/**
 * Modal component for viewing a document in-place without navigating away.
 * Loads document data lazily when the modal opens.
 */
export const DocumentModal: React.FC<DocumentViewModalProps> = ({
    documentUrn: initialDocumentUrn,
    onClose,
    onDocumentDeleted,
}) => {
    // Use state to allow breadcrumb navigation within the modal
    const [currentDocumentUrn, setCurrentDocumentUrn] = React.useState(initialDocumentUrn);

    // Update current document URN when initial prop changes
    React.useEffect(() => {
        setCurrentDocumentUrn(initialDocumentUrn);
    }, [initialDocumentUrn]);

    // Lazy load document data when modal opens or when document URN changes
    const { data, loading, refetch } = useGetDocumentQuery({
        variables: { urn: currentDocumentUrn, includeParentDocuments: true },
        skip: !currentDocumentUrn,
    });

    const document = data?.document;

    const wrappedRefetch = async () => {
        return refetch();
    };

    const handleDelete = React.useCallback(
        (_deletedNode: DocumentTreeNode | null) => {
            // Delete mutation is handled in DocumentActionsMenu
            if (onDocumentDeleted) {
                onDocumentDeleted();
            }
            onClose();
        },
        [onClose, onDocumentDeleted],
    );

    return (
        <StyledModal
            title="" // Empty title, header will be hidden via CSS
            onCancel={onClose}
            buttons={[]}
            width="90%"
            style={{ maxWidth: '1200px' }}
            dataTestId="document-view-modal"
        >
            <ModalContent>
                {loading || !document ? (
                    <LoadingWrapper>
                        <LoadingOutlined style={{ fontSize: 36, color: colors.gray[200] }} />
                    </LoadingWrapper>
                ) : (
                    <EntityContext.Provider
                        value={{
                            urn: currentDocumentUrn,
                            entityType: EntityType.Document,
                            entityData: document as any,
                            loading: false,
                            baseEntity: document as any,
                            dataNotCombinedWithSiblings: undefined,
                            routeToTab: () => {},
                            refetch: wrappedRefetch,
                            lineage: undefined,
                        }}
                    >
                        <PageTemplateProvider templateType={PageTemplateSurfaceType.AssetSummary}>
                            <ContentWrapper>
                                <DocumentSummaryTab onDelete={handleDelete} />
                            </ContentWrapper>
                        </PageTemplateProvider>
                    </EntityContext.Provider>
                )}
            </ModalContent>
        </StyledModal>
    );
};
