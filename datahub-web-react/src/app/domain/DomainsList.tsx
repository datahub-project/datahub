import React, { useState } from 'react';
import { Button, Empty, List, message, Pagination, Typography } from 'antd';
import styled from 'styled-components';
import { PlusOutlined } from '@ant-design/icons';
import { Domain } from '../../types.generated';
import { useListDomainsQuery } from '../../graphql/domain.generated';
import CreateDomainModal from './CreateDomainModal';
import { Message } from '../shared/Message';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import DomainListItem from './DomainListItem';

const DomainsContainer = styled.div``;

const DomainsStyledList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
    }
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
    const [page, setPage] = useState(1);
    const [isCreatingDomain, setIsCreatingDomain] = useState(false);

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const { loading, error, data, refetch } = useListDomainsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
            },
        },
        fetchPolicy: 'no-cache',
    });

    const totalDomains = data?.listDomains?.total || 0;
    const lastResultIndex = start + pageSize > totalDomains ? totalDomains : start + pageSize;
    const domains = (data?.listDomains?.domains || []).sort(
        (a, b) => (b.entities?.total || 0) - (a.entities?.total || 0),
    );

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    // TODO: Handle robust deleting of domains.

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading domains..." />}
            {error && message.error({ content: `Failed to load domains: \n ${error.message || ''}`, duration: 3 })}
            <DomainsContainer>
                <TabToolbar>
                    <div>
                        <Button type="text" onClick={() => setIsCreatingDomain(true)}>
                            <PlusOutlined /> New Domain
                        </Button>
                    </div>
                </TabToolbar>
                <DomainsStyledList
                    bordered
                    locale={{
                        emptyText: <Empty description="No Domains!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    dataSource={domains}
                    renderItem={(item: any) => <DomainListItem domain={item as Domain} />}
                />
                <DomainsPaginationContainer>
                    <PaginationInfo>
                        <b>
                            {lastResultIndex > 0 ? (page - 1) * pageSize + 1 : 0} - {lastResultIndex}
                        </b>{' '}
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
                <CreateDomainModal
                    visible={isCreatingDomain}
                    onClose={() => setIsCreatingDomain(false)}
                    onCreate={() => {
                        // Hack to deal with eventual consistency.
                        setTimeout(function () {
                            refetch?.();
                        }, 2000);
                    }}
                />
            </DomainsContainer>
        </>
    );
};
