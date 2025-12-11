/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useContext } from 'react';
import styled from 'styled-components';

import CompactTagProfile from '@app/entityV2/tag/CompactTagProfile';
import CompactContext from '@app/shared/CompactContext';
import { Message } from '@app/shared/Message';
import TagStyleEntity from '@app/shared/TagStyleEntity';

import { useGetTagQuery } from '@graphql/tag.generated';

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
