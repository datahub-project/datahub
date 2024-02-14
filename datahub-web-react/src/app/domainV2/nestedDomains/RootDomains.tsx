import React from 'react';
import styled from 'styled-components';
import { ReadOutlined } from '@ant-design/icons';
import { Message } from '../../shared/Message';
import { useEntityRegistry } from '../../useEntityRegistry';
import { EntityType } from '../../../types.generated';
import useListDomains from '../useListDomains';
import EmptyDomainsSection from '../EmptyDomainsSection';
import EmptyDomainDescription from '../EmptyDomainDescription';

const DomainsWrapper = styled.div`
    overflow: auto;
    padding: 0 28px 16px 28px;
`;

const ResultWrapper = styled.div`
    padding: 20px;
    margin: 16px;
    display: flex;
    align-items: center;
    background-color: #ffffff;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0px 0px 5px rgba(0, 0, 0, 0.08);
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
                    <ResultWrapper>
                        {entityRegistry.renderSearchResult(EntityType.Domain, { entity: domain, matchedFields: [] })}
                    </ResultWrapper>
                ))}
            </DomainsWrapper>
        </>
    );
}
