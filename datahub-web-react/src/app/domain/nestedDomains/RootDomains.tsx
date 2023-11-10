import React from 'react';
import styled from 'styled-components';
import { Message } from '../../shared/Message';
import { ResultWrapper } from '../../search/SearchResultList';
import { useEntityRegistry } from '../../useEntityRegistry';
import { EntityType } from '../../../types.generated';
import useListDomains from '../useListDomains';
import EmptyDomainsSection from '../EmptyDomainsSection';

const DomainsWrapper = styled.div`
    overflow: auto;
    padding: 0 28px 16px 28px;
`;

interface Props {
    setIsCreatingDomain: React.Dispatch<React.SetStateAction<boolean>>;
}
export default function RootDomains({ setIsCreatingDomain }: Props) {
    const entityRegistry = useEntityRegistry();
    const { loading, error, data, sortedDomains } = useListDomains({});

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading domains..." />}
            {error && <Message type="error" content="Failed to load domains. An unexpected error occurred." />}
            {!loading && (!data || !data?.listDomains?.domains?.length) && (
                <EmptyDomainsSection setIsCreatingDomain={setIsCreatingDomain} />
            )}
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
