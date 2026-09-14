import { LoadingOutlined } from '@ant-design/icons';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import { useContextLayout } from '@app/context/ContextLayoutContext';
import { DocumentExternalProfile } from '@app/entityV2/document/DocumentExternalProfile';
import { DocumentNativeProfile } from '@app/entityV2/document/DocumentNativeProfile';

import { useGetDocumentQuery } from '@graphql/document.generated';
import { DocumentSourceType } from '@types';

const LoadingWrapper = styled.div`
    align-items: center;
    justify-content: center;
    height: 100%;
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    border-radius: 12px;
    padding: 20px;
    margin: 4px;
    background-color: ${(props) => props.theme.colors.bg};
    box-shadow: ${(props) => props.theme.colors.shadowSm};
`;

/**
 * Wrapper component that determines which profile to render based on document source type:
 * - NATIVE: Uses DocumentSimpleProfile (custom document editor)
 * - EXTERNAL: Uses DocumentExternalProfile (traditional EntityProfile with tabs)
 */
export const DocumentProfile = ({ urn }: { urn: string }): JSX.Element => {
    const { data, loading, refetch } = useGetDocumentQuery({
        variables: { urn, includeParentDocuments: true },
    });
    const contextLayout = useContextLayout();

    const document = data?.document;
    const sourceType = document?.info?.source?.sourceType;
    const isExternal = sourceType === DocumentSourceType.External;

    // Sidebar is hidden by default (set in ContextRoutes). Show it for any
    // document (native or external) once the document has loaded — the tree
    // surfaces both native + ingested-platform docs, so we want it visible
    // regardless of source type.
    useEffect(() => {
        if (document && contextLayout?.setSidebarHidden) {
            contextLayout.setSidebarHidden(false);
        }
    }, [document, contextLayout]);

    if (loading || !document) {
        return (
            <LoadingWrapper>
                <LoadingOutlined style={{ fontSize: 36 }} />
            </LoadingWrapper>
        );
    }

    // For external documents, use the traditional EntityProfile
    if (isExternal) {
        return <DocumentExternalProfile urn={urn} />;
    }

    // For native documents (or documents without source type), use the custom DocumentNativeProfile
    return <DocumentNativeProfile urn={urn} document={document as any} refetch={refetch} />;
};
