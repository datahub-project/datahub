import React, { useEffect, useState } from 'react';
import { Button, Empty, Pagination, Tag, Tooltip, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { useLocation } from 'react-router';
import styled from 'styled-components';
import * as QueryString from 'query-string';
import { PlusOutlined } from '@ant-design/icons';
import { AlignType } from 'rc-table/lib/interface';
import { EntityType, Maybe, Ownership } from '../../types.generated';
import { useListDomainsQuery } from '../../graphql/domain.generated';
import CreateDomainModal from './CreateDomainModal';
import { Message } from '../shared/Message';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import { SearchBar } from '../search/SearchBar';
import { useEntityRegistry } from '../useEntityRegistry';
import { scrollToTop } from '../shared/searchUtils';
import { addToListDomainsCache, removeFromListDomainsCache } from './utils';
import { OnboardingTour } from '../onboarding/OnboardingTour';
import { DOMAINS_INTRO_ID, DOMAINS_CREATE_DOMAIN_ID } from '../onboarding/config/DomainsOnboardingConfig';
import { ANTD_GRAY, getElasticCappedTotalValueText } from '../entity/shared/constants';
import { StyledTable } from '../entity/shared/components/styled/StyledTable';
import DomainItemDelete from './DomainItemDelete';
import DomainOwners from './DomainOwners';
import { IconStyleType } from '../entity/Entity';

export interface DomainEntry {
    name: string;
    entities: string;
    urn: string;
    owners?: Maybe<Ownership>;
    url: string;
}

const NoData = styled(Empty)`
    color: ${ANTD_GRAY[6]};
    padding-top: 60px;
`;

const DomainsContainer = styled.div``;

const DomainNameContainer = styled.div`
    margin-left: 16px;
    margin-right: 16px;
    display: inline;
`;

const DomainsPaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    padding: 12px;
    padding-left: 16px;
    border-bottom: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const PaginationInfo = styled(Typography.Text)`
    padding: 0px;
`;

const DEFAULT_PAGE_SIZE = 25;

export const DomainsList = () => {
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const [page, setPage] = useState(1);
    const [isCreatingDomain, setIsCreatingDomain] = useState(false);

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const { loading, error, data, client, refetch } = useListDomainsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query,
            },
        },
        fetchPolicy: 'cache-first',
    });

    const totalDomains = data?.listDomains?.total || 0;
    const lastResultIndex = start + pageSize > totalDomains ? totalDomains : start + pageSize;
    const domains = data?.listDomains?.domains || [];

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleDelete = (urn: string) => {
        removeFromListDomainsCache(client, urn, page, pageSize, query);
        setTimeout(function () {
            refetch?.();
        }, 2000);
    };

    const logoIcon = entityRegistry.getIcon(EntityType.Domain, 12, IconStyleType.ACCENT);
    const allColumns = [
        {
            title: 'Name',
            dataIndex: '',
            key: 'name',
            sorter: (sourceA, sourceB) => {
                return sourceA.name.localeCompare(sourceB.name);
            },
            render(record: DomainEntry) {
                return (
                    <Link to={record.url}>
                        {logoIcon}
                        <DomainNameContainer>
                            <Typography.Text>{record.name}</Typography.Text>
                        </DomainNameContainer>
                        <Tooltip title={`There are ${record.entities} entities in this domain.`}>
                            <Tag>{record.entities} entities</Tag>
                        </Tooltip>
                    </Link>
                );
            },
        },
        {
            title: 'Owners',
            dataIndex: '',
            width: '10%',
            key: 'owners',
            render(record: DomainEntry) {
                return (
                    <>
                        <DomainOwners ownership={record.owners} />
                    </>
                );
            },
        },
        {
            title: '',
            dataIndex: '',
            width: '5%',
            align: 'right' as AlignType,
            key: 'menu',
            render(record: DomainEntry) {
                return (
                    <>
                        <DomainItemDelete
                            name={record.name}
                            urn={record.urn}
                            onDelete={() => handleDelete(record.urn)}
                        />
                    </>
                );
            },
        },
    ];
    const domainData: Array<DomainEntry> = [];
    for (let i = 0; i < domains.length; i++) {
        const domain = domains[i];
        const displayName = entityRegistry.getDisplayName(EntityType.Domain, domain);
        const totalEntitiesText = getElasticCappedTotalValueText(domain.entities?.total || 0);
        const url = entityRegistry.getEntityUrl(EntityType.Domain, domain.urn);

        domainData.push({
            name: displayName,
            entities: totalEntitiesText,
            urn: domain.urn,
            owners: domain.ownership,
            url,
        });
    }

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading domains..." />}
            {error && <Message type="error" content="Failed to load domains! An unexpected error occurred." />}
            <OnboardingTour stepIds={[DOMAINS_INTRO_ID, DOMAINS_CREATE_DOMAIN_ID]} />
            <DomainsContainer>
                <TabToolbar>
                    <Button id={DOMAINS_CREATE_DOMAIN_ID} type="text" onClick={() => setIsCreatingDomain(true)}>
                        <PlusOutlined /> New Domain
                    </Button>
                    <SearchBar
                        initialQuery={query || ''}
                        placeholderText="Search domains..."
                        suggestions={[]}
                        style={{
                            maxWidth: 220,
                            padding: 0,
                        }}
                        inputStyle={{
                            height: 32,
                            fontSize: 12,
                        }}
                        onSearch={() => null}
                        onQueryChange={(q) => setQuery(q)}
                        entityRegistry={entityRegistry}
                        hideRecommendations
                    />
                </TabToolbar>
                {domains && domains.length > 0 ? (
                    <StyledTable columns={allColumns} dataSource={domainData} rowKey="urn" pagination={false} />
                ) : (
                    <NoData description="No Domains!" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                )}
                <DomainsPaginationContainer>
                    <PaginationInfo>
                        <b>
                            {lastResultIndex > 0 ? (page - 1) * pageSize + 1 : 0} - {lastResultIndex}
                        </b>
                        of <b>{totalDomains}</b>
                    </PaginationInfo>
                    <Pagination
                        current={page}
                        pageSize={pageSize}
                        total={totalDomains}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                    <span />
                </DomainsPaginationContainer>
                {isCreatingDomain && (
                    <CreateDomainModal
                        onClose={() => setIsCreatingDomain(false)}
                        onCreate={(urn, _, name, description) => {
                            addToListDomainsCache(
                                client,
                                {
                                    urn,
                                    properties: {
                                        name,
                                        description,
                                    },
                                    ownership: null,
                                    entities: null,
                                },
                                pageSize,
                                query,
                            );
                            setTimeout(() => refetch(), 2000);
                        }}
                    />
                )}
            </DomainsContainer>
        </>
    );
};
