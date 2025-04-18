import React, { useContext } from 'react';
import styled from 'styled-components';
import { Message } from '../../shared/Message';
import TagStyleEntity from '../../shared/TagStyleEntity';
import { useGetTagQuery } from '../../../graphql/tag.generated';
import CompactContext from '../../shared/CompactContext';
import CompactTagProfile from './CompactTagProfile';

const PageContainer = styled.div`
    padding: 32px 100px;
    background-color: white;
    border-radius: 8px;
`;

const LoadingMessage = styled(Message)`
    margin-top: 10%;
`;

interface Props {
    urn: string;
}

/**
 * Responsible for displaying metadata about a tag
 */
export default function TagProfile({ urn }: Props) {
    const { loading } = useGetTagQuery({ variables: { urn } });
    const isCompact = useContext(CompactContext);

    if (isCompact) {
        return <CompactTagProfile urn={urn} />;
    }

    return (
        <PageContainer>
            {loading && <LoadingMessage type="loading" content="Loading..." />}
            <TagStyleEntity urn={urn} />
        </PageContainer>
    );
}
