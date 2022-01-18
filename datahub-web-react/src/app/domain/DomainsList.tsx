import React, { useState } from 'react';
import { Button, Empty, List, message, Pagination } from 'antd';
import styled from 'styled-components';
import { PlusOutlined } from '@ant-design/icons';
import { Domain } from '../../types.generated';
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

    const totalDomains = data?.listGroups?.total || 0;
    const domains = data?.listGroups?.domains || [];

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
                    <Pagination
                        style={{ margin: 40 }}
                        current={page}
                        pageSize={pageSize}
                        total={totalDomains}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
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
