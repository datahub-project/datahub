import { LoadingOutlined } from '@ant-design/icons';
import { Result } from 'antd';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { Redirect } from 'react-router-dom';
import styled from 'styled-components';

import { useContextDocumentsPermissions } from '@app/context/useContextDocumentsPermissions';
import { useDocumentTree } from '@app/document/DocumentTreeContext';
import { useCreateDocumentTreeMutation } from '@app/document/hooks/useDocumentTreeMutations';
import { useLoadDocumentTree } from '@app/document/hooks/useLoadDocumentTree';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { colors } from '@src/alchemy-components';

import { EntityType } from '@types';

const ContentCard = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    flex-direction: column;
    gap: 16px;
    height: 100%;
    width: 100%;
    background-color: #ffffff;
    border-radius: 12px;
    box-shadow: 0 0 6px 0px rgba(93, 102, 139, 0.2);
    margin: 4px;
`;

/**
 * ContextDocumentsPage - Landing page for /context/documents
 *
 * Behavior:
 * 1. If documents exist: redirect to the first document
 * 2. If no documents AND user can create: auto-create a new document and redirect
 * 3. If no documents AND user cannot create: show unauthorized/empty state
 */
export default function ContextDocumentsPage() {
    const entityRegistry = useEntityRegistry();
    const { getRootNodes } = useDocumentTree();
    const { loading: treeLoading } = useLoadDocumentTree();
    const { createDocument } = useCreateDocumentTreeMutation();

    const { canCreate: canCreateDocuments } = useContextDocumentsPermissions();

    const [isCreating, setIsCreating] = useState(false);
    const [redirectUrn, setRedirectUrn] = useState<string | null>(null);
    const hasAttemptedAction = useRef(false);

    const rootNodes = getRootNodes();

    const handleAutoCreate = useCallback(async () => {
        if (hasAttemptedAction.current) return;
        hasAttemptedAction.current = true;

        setIsCreating(true);
        try {
            const newUrn = await createDocument({
                title: 'New Document',
                parentDocument: null,
            });

            if (newUrn) {
                setRedirectUrn(newUrn);
            }
        } catch (error) {
            console.error('Failed to create document:', error);
            // Reset so user can try again
            hasAttemptedAction.current = false;
        } finally {
            setIsCreating(false);
        }
    }, [createDocument]);

    useEffect(() => {
        // Wait for tree to load
        if (treeLoading) return;

        // If we already have a redirect URN, don't do anything
        if (redirectUrn) return;

        // If documents exist, redirect to the first one
        if (rootNodes.length > 0) {
            if (!hasAttemptedAction.current) {
                hasAttemptedAction.current = true;
                setRedirectUrn(rootNodes[0].urn);
            }
            return;
        }

        // No documents exist - check permissions
        if (canCreateDocuments && !isCreating) {
            // Auto-create a new document
            handleAutoCreate();
        }
    }, [treeLoading, rootNodes, canCreateDocuments, isCreating, handleAutoCreate, redirectUrn]);

    // Redirect to document if we have a URN
    if (redirectUrn) {
        const url = entityRegistry.getEntityUrl(EntityType.Document, redirectUrn);
        return <Redirect to={url} />;
    }

    // Still loading tree
    if (treeLoading) {
        return (
            <ContentCard data-testid="context-documents-loading">
                <LoadingOutlined style={{ fontSize: 36, color: colors.gray[400] }} />
            </ContentCard>
        );
    }

    // Creating a new document
    if (isCreating) {
        return (
            <ContentCard data-testid="context-documents-creating">
                <LoadingOutlined style={{ fontSize: 36, color: colors.gray[400] }} />
            </ContentCard>
        );
    }

    // No documents and user cannot create - show unauthorized state
    if (rootNodes.length === 0 && !canCreateDocuments) {
        return (
            <ContentCard data-testid="context-documents-empty-unauthorized">
                <Result
                    status="403"
                    title="No Documents Available"
                    subTitle="There are no documents available. Contact your DataHub administrator to get started."
                />
            </ContentCard>
        );
    }

    // Fallback loading state while redirecting
    return (
        <ContentCard data-testid="context-documents-redirecting">
            <LoadingOutlined style={{ fontSize: 36, color: colors.gray[400] }} />
        </ContentCard>
    );
}
