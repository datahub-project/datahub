import React from 'react';
import styled from 'styled-components';
import { useListDomainsQuery } from '../../../graphql/domain.generated';
import { Message } from '../../shared/Message';
import { ResultWrapper } from '../../search/SearchResultList';
import { useEntityRegistry } from '../../useEntityRegistry';
import { EntityType } from '../../../types.generated';

const RootDomainsWrapper = styled.div`
    padding: 0 28px;
`;

const RootDomainsHeader = styled.div`
    font-size: 20px;
    margin-bottom: 18px;
`;

export default function RootDomains() {
    const entityRegistry = useEntityRegistry();
    const { loading, error, data } = useListDomainsQuery({
        variables: {
            input: {
                start: 0,
                count: 1000, // don't paginate the home page, get all root level domains
            },
        },
    });

    return (
        <RootDomainsWrapper>
            <RootDomainsHeader>Your Domains</RootDomainsHeader>
            {!data && loading && <Message type="loading" content="Loading domains..." />}
            {error && <Message type="error" content="Failed to load domains. An unexpected error occurred." />}
            {data?.listDomains?.domains.map((domain) => (
                <ResultWrapper showUpdatedStyles>
                    {entityRegistry.renderSearchResult(EntityType.Domain, { entity: domain, matchedFields: [] })}
                </ResultWrapper>
            ))}
        </RootDomainsWrapper>
    );
}
