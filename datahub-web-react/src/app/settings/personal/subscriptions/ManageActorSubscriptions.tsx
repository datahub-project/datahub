import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Pagination, Table, Typography } from 'antd';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { useListSubscriptionsQuery } from '../../../../graphql/subscriptions.generated';
import { ReactComponent as EmptySimpleSvg } from '../../../../images/empty-simple.svg';
import { EntityColumn } from './table/EntityColumn';
import { UpstreamsColumn } from './table/UpstreamsColumn';
import { EditSubscriptionColumn } from './table/EditSubscriptionColumn';
import { SubscribedSinceColumn } from './table/SubscribedSinceColumn';
import { scrollToTop } from '../../../shared/searchUtils';

const PAGE_SIZE = 10;

const PageContainer = styled.div`
    padding-top: 20px;
    width: 100%;
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 24px;
    }
`;

const SubscriptionsTitle = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 24px;
    line-height: 32px;
    font-weight: 400;
`;

const SubscriptionsTable = styled(Table)`
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 8px;
    margin-top: 12px;
    margin-right: 24px;
    align-self: flex-end;
`;

const ColumnTitle = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 700;
    color: rgba(0, 0, 0, 0.88);
    display: flex;
    align-items: center;
`;

const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 12px;
    padding-top: 40px;
    padding-bottom: 40px;
`;

const EmptySubscriptionsText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 700;
    color: #595959;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const StyledPagination = styled(Pagination)`
    margin: 40px;
`;

type Props = {
    isPersonal: boolean;
    groupUrn?: string;
};

/**
 * Component used for managing actor subscriptions.
 */
export const ManageActorSubscriptions = ({ isPersonal, groupUrn }: Props) => {
    const [page, setPage] = useState(1);
    const start = (page - 1) * PAGE_SIZE;
    const { data: listSubscriptionData, refetch: refetchListSubscriptions } = useListSubscriptionsQuery({
        variables: { input: { start, count: PAGE_SIZE, groupUrn: groupUrn || undefined } },
    });
    const subscriptions = listSubscriptionData?.listSubscriptions?.subscriptions || [];
    const numSubscriptions = listSubscriptionData?.listSubscriptions?.subscriptions?.length || 0;
    const pageTitle = isPersonal ? 'My Subscriptions' : 'Group Subscriptions';
    const subscriptionTableColumns = [
        {
            title: <ColumnTitle>Entity</ColumnTitle>,
            dataIndex: 'Entity',
            key: 'entity',
            sorter: (a: any, b: any) => a?.entityName?.localeCompare(b?.entityName),
            render: (_, record: any) => <EntityColumn subscription={record} />,
        },
        {
            title: <ColumnTitle>Subscribed to Upstreams</ColumnTitle>,
            dataIndex: 'upstreams',
            key: 'upstreams',
            render: (_, record: any) => <UpstreamsColumn subscription={record} />,
        },
        {
            title: <ColumnTitle>Subscribed Since</ColumnTitle>,
            dataIndex: 'since',
            key: 'since',
            sorter: (a: any, b: any) => a?.createdOn?.time - b?.cratedOn?.time,
            render: (_, record: any) => <SubscribedSinceColumn subscription={record} />,
        },
        {
            title: <ColumnTitle>Edit Subscription</ColumnTitle>,
            dataIndex: 'edit',
            key: 'edit',
            render: (_, record: any) => (
                <EditSubscriptionColumn
                    subscription={record}
                    refetchListSubscriptions={refetchListSubscriptions}
                    isPersonal={isPersonal}
                    groupUrn={groupUrn}
                />
            ),
        },
    ];

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    return (
        <PageContainer>
            <PageHeaderContainer>
                <SubscriptionsTitle>{pageTitle}</SubscriptionsTitle>
                <SubscriptionsTable
                    columns={subscriptionTableColumns}
                    dataSource={subscriptions}
                    rowKey="urn"
                    locale={{
                        emptyText: (
                            <EmptyContainer>
                                <EmptySimpleSvg />
                                <EmptySubscriptionsText>
                                    You are not currently subscribed to any entities. Get started by subscribing to
                                    entities most relevant to you.
                                </EmptySubscriptionsText>
                            </EmptyContainer>
                        ),
                    }}
                    pagination={false}
                />
                {numSubscriptions >= PAGE_SIZE && (
                    <PaginationContainer>
                        <StyledPagination
                            current={page}
                            pageSize={PAGE_SIZE}
                            total={numSubscriptions}
                            showLessItems
                            onChange={onChangePage}
                            showSizeChanger={false}
                        />
                    </PaginationContainer>
                )}
            </PageHeaderContainer>
        </PageContainer>
    );
};
