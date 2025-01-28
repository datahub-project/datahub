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
                    <ResultWrapper key={domain.urn}>
                        {entityRegistry.renderSearchResult(EntityType.Domain, { entity: domain, matchedFields: [] })}
                    </ResultWrapper>
                ))}
            </DomainsWrapper>
        </>
    );
}
