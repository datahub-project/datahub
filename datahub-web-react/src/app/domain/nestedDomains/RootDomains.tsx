import React from 'react';
import styled from 'styled-components';
import { Message } from '../../shared/Message';
import { ResultWrapper } from '../../search/SearchResultList';
import { useEntityRegistry } from '../../useEntityRegistry';
import { EntityType } from '../../../types.generated';
import useListDomains from '../useListDomains';

const RootDomainsHeader = styled.div`
    font-size: 20px;
    margin-bottom: 18px;
    padding: 0 28px;
`;

const DomainsWrapper = styled.div`
    overflow: auto;
    padding: 0 28px 16px 28px;
`;

export default function RootDomains() {
    const entityRegistry = useEntityRegistry();
    const { loading, error, data, sortedDomains } = useListDomains({});

    return (
        <>
            <RootDomainsHeader>Your Domains</RootDomainsHeader>
            {!data && loading && <Message type="loading" content="Loading domains..." />}
            {error && <Message type="error" content="Failed to load domains. An unexpected error occurred." />}
            <DomainsWrapper>
                {sortedDomains?.map((domain) => (
                    <ResultWrapper showUpdatedStyles>
                        {entityRegistry.renderSearchResult(EntityType.Domain, { entity: domain, matchedFields: [] })}
                    </ResultWrapper>
                ))}
            </DomainsWrapper>
        </>
    );
}
