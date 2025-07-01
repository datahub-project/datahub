import React from 'react';
import { useParams } from 'react-router';
import styled from 'styled-components';

import { decodeUrn } from '@app/entity/shared/utils';
import { Message } from '@app/shared/Message';
import TagStyleEntity from '@app/shared/TagStyleEntity';

import { useGetTagQuery } from '@graphql/tag.generated';

const PageContainer = styled.div`
    padding: 32px 100px;
`;

const LoadingMessage = styled(Message)`
    margin-top: 10%;
`;

type TagPageParams = {
    urn: string;
};

/**
 * Responsible for displaying metadata about a tag
 */
export default function TagProfile() {
    const { urn: encodedUrn } = useParams<TagPageParams>();
    const urn = decodeUrn(encodedUrn);
    const { loading } = useGetTagQuery({ variables: { urn } });

    return (
        <PageContainer>
            {loading && <LoadingMessage type="loading" content="Loading..." />}
            <TagStyleEntity urn={urn} />
        </PageContainer>
    );
}
