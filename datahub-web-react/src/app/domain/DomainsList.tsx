import { PlusOutlined } from '@ant-design/icons';
import { Button, Empty, Pagination, Typography } from 'antd';
import * as QueryString from 'query-string';
import { AlignType } from 'rc-table/lib/interface';
import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import CreateDomainModal from '@app/domain/CreateDomainModal';
import DomainIcon from '@app/domain/DomainIcon';
import { DomainListMenuColumn, DomainNameColumn, DomainOwnersColumn } from '@app/domain/DomainListColumns';
import { addToListDomainsCache, removeFromListDomainsCache } from '@app/domain/utils';
import { StyledTable } from '@app/entity/shared/components/styled/StyledTable';
import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import { getElasticCappedTotalValueText } from '@app/entity/shared/constants';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { DOMAINS_CREATE_DOMAIN_ID, DOMAINS_INTRO_ID } from '@app/onboarding/config/DomainsOnboardingConfig';
import { SearchBar } from '@app/search/SearchBar';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useListDomainsQuery } from '@graphql/domain.generated';
import { EntityType } from '@types';

const DomainsContainer = styled.div``;

export const DomainsPaginationContainer = styled.div`
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
        fetchPolicy: query && query.length > 0 ? 'no-cache' : 'cache-first',
    });

    const totalDomains = data?.listDomains?.total || 0;
    const lastResultIndex = start + pageSize > totalDomains ? totalDomains : start + pageSize;
    const domains = data?.listDomains?.domains || [];

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleDelete = (urn: string) => {
        removeFromListDomainsCache(client, urn, page, pageSize);
        setTimeout(() => {
            refetch?.();
        }, 2000);
    };

    const allColumns = [
        {
            title: 'Name',
            dataIndex: '',
            key: 'name',
            sorter: (sourceA, sourceB) => {
                return sourceA.name.localeCompare(sourceB.name);
            },
            render: DomainNameColumn(
                <DomainIcon
                    style={{
                        fontSize: 12,
                        color: '#BFBFBF',
                    }}
                />,
            ),
        },
        {
            title: 'Owners',
            dataIndex: 'ownership',
            width: '10%',
            key: 'ownership',
            render: DomainOwnersColumn,
        },
        {
            title: '',
            dataIndex: '',
            width: '5%',
            align: 'right' as AlignType,
            key: 'menu',
            render: DomainListMenuColumn(handleDelete),
        },
    ];

    const tableData = domains.map((domain) => {
        const displayName = entityRegistry.getDisplayName(EntityType.Domain, domain);
        const totalEntitiesText = getElasticCappedTotalValueText(domain.entities?.total || 0);
        const url = entityRegistry.getEntityUrl(EntityType.Domain, domain.urn);

        return {
            urn: domain.urn,
            name: displayName,
            entities: totalEntitiesText,
            ownership: domain.ownership,
            url,
        };
    });

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
                        onQueryChange={(q) => setQuery(q && q.length > 0 ? q : undefined)}
                        entityRegistry={entityRegistry}
                        hideRecommendations
                    />
                </TabToolbar>
                <StyledTable
                    columns={allColumns}
                    dataSource={tableData}
                    rowKey="urn"
                    pagination={false}
                    locale={{ emptyText: <Empty description="No Domains!" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
                />
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
                                        description: description || null,
                                    },
                                    ownership: null,
                                    entities: null,
                                    displayProperties: null,
                                },
                                pageSize,
                            );
                            setTimeout(() => refetch(), 2000);
                        }}
                    />
                )}
            </DomainsContainer>
        </>
    );
};
