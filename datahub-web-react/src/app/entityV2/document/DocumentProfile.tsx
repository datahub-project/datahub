import { LoadingOutlined } from '@ant-design/icons';
import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

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
    background-color: #ffffff;
    box-shadow: 0 0 6px 0px rgba(93, 102, 139, 0.2);
`;

/**
 * Wrapper component that determines which profile to render based on document source type:
 * - NATIVE: Uses DocumentSimpleProfile (custom document editor)
 * - EXTERNAL: Uses DocumentExternalProfile (traditional EntityProfile with tabs)
 */
export const DocumentProfile = ({ urn }: { urn: string }): JSX.Element => {
    const { data, loading, refetch } = useGetDocumentQuery({
        variables: { urn },
    });

    const document = data?.document;

    if (loading || !document) {
        return (
            <LoadingWrapper>
                <LoadingOutlined style={{ fontSize: 36, color: colors.gray[200] }} />
            </LoadingWrapper>
        );
    }

    const sourceType = document.info?.source?.sourceType;
    const isExternal = sourceType === DocumentSourceType.External;

    // For external documents, use the traditional EntityProfile
    if (isExternal) {
        return <DocumentExternalProfile urn={urn} />;
    }

    // For native documents (or documents without source type), use the custom DocumentNativeProfile
    return <DocumentNativeProfile urn={urn} document={document as any} refetch={refetch} />;
};
