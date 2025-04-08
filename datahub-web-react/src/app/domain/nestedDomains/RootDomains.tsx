import { ReadOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import EmptyDomainDescription from '@app/domain/EmptyDomainDescription';
import EmptyDomainsSection from '@app/domain/EmptyDomainsSection';
import useListDomains from '@app/domain/useListDomains';
import { ResultWrapper } from '@app/search/SearchResultList';
import { Message } from '@app/shared/Message';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

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
                <EmptyDomainsSection
                    icon={<ReadOutlined />}
                    title="Organize your data"
                    description={<EmptyDomainDescription />}
                    setIsCreatingDomain={setIsCreatingDomain}
                />
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
