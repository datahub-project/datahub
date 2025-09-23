import { ReadOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import EmptyDomainDescription from '@app/domainV2/EmptyDomainDescription';
import EmptyDomainsSection from '@app/domainV2/EmptyDomainsSection';
import useScrollDomains from '@app/domainV2/useScrollDomains';
import Loading from '@app/shared/Loading';
import { Message } from '@app/shared/Message';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const DomainsWrapper = styled.div`
    overflow: auto;
    padding: 0 20px 12px 20px;
`;

const ResultWrapper = styled.div`
    padding: 16px;
    margin: 0px;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    background-color: #ffffff;
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
    border: 1px solid #ebecf0;
`;

const LoadingWrapper = styled.div`
    padding: 16px;
`;

interface Props {
    setIsCreatingDomain: React.Dispatch<React.SetStateAction<boolean>>;
}
export default function RootDomains({ setIsCreatingDomain }: Props) {
    const entityRegistry = useEntityRegistry();
    const { domains, hasInitialized, loading, error, scrollRef } = useScrollDomains({});

    return (
        <>
            {error && <Message type="error" content="Failed to load domains. An unexpected error occurred." />}
            {hasInitialized && domains.length === 0 && (
                <EmptyDomainsSection
                    icon={<ReadOutlined />}
                    title="Organize your data"
                    description={<EmptyDomainDescription />}
                    setIsCreatingDomain={setIsCreatingDomain}
                />
            )}
            <DomainsWrapper>
                {domains?.map((domain) => (
                    <ResultWrapper key={domain.urn}>
                        {entityRegistry.renderSearchResult(EntityType.Domain, { entity: domain, matchedFields: [] })}
                    </ResultWrapper>
                ))}
                {loading && (
                    <LoadingWrapper>
                        <Loading height={24} marginTop={0} />
                    </LoadingWrapper>
                )}
                {domains.length > 0 && <div ref={scrollRef} />}
            </DomainsWrapper>
        </>
    );
}
