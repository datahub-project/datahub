import React from 'react';

import styled from 'styled-components';

import { Message } from '../../shared/Message';
import TagStyleEntity from '../../shared/TagStyleEntity';
import { useGetTagQuery } from '../../../graphql/tag.generated';

const PageContainer = styled.div`
    padding: 32px 100px;
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

    return (
        <PageContainer>
            {loading && <LoadingMessage type="loading" content="Loading..." />}
            <TagStyleEntity urn={urn} />
        </PageContainer>
    );
}
