import React from 'react';

import { useParams } from 'react-router';
import styled from 'styled-components/macro';
import { decodeUrn } from '../shared/utils';
import TagStyleEntity from '../../shared/TagStyleEntity';

const PageContainer = styled.div`
    padding: 32px 100px;
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

    return (
        <PageContainer>
            <TagStyleEntity urn={urn} />
        </PageContainer>
    );
}
