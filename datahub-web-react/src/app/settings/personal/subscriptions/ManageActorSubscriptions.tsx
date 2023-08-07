import { LoadingOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Pagination, Table, Typography } from 'antd';
import { ColumnsType } from 'antd/lib/table';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { useListSubscriptionsQuery } from '../../../../graphql/subscriptions.generated';
import { ReactComponent as EmptySimpleSvg } from '../../../../images/empty-simple.svg';
import { EntityColumn } from './table/EntityColumn';
import { UpstreamsColumn } from './table/UpstreamsColumn';
import { EditSubscriptionColumn } from './table/EditSubscriptionColumn';
import { SubscribedSinceColumn } from './table/SubscribedSinceColumn';
import { scrollToTop } from '../../../shared/searchUtils';
import { ENABLE_UPSTREAM_NOTIFICATIONS } from '../notifications/constants';
import ChannelColumn from './table/ChannelColumn';
import useSinkSettings from '../../../shared/subscribe/drawer/useSinkSettings';
import { DataHubSubscription } from '../../../../types.generated';

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
    margin-top: 24px;
    margin-right: 24px;
    align-self: flex-end;
    && tbody > tr:hover > td {
        background: inherit;
    }
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
    const {
        data: listSubscriptionData,
        loading,
        refetch: refetchListSubscriptions,
    } = useListSubscriptionsQuery({
        variables: { input: { start, count: PAGE_SIZE, groupUrn: groupUrn || undefined } },
    });
    const { settingsChannel } = useSinkSettings({ isPersonal, groupUrn });
    const subscriptions = listSubscriptionData?.listSubscriptions?.subscriptions || [];
    const numSubscriptions = listSubscriptionData?.listSubscriptions?.total || 0;
    const pageTitle = isPersonal ? 'My Subscriptions' : 'Group Subscriptions';
    const subscriptionTableColumns: ColumnsType<DataHubSubscription> = [
        {
            title: <ColumnTitle>Entity</ColumnTitle>,
            dataIndex: 'Entity',
            key: 'entity',
            render: (_, subscription: DataHubSubscription) => <EntityColumn subscription={subscription} />,
        },
        {
            title: <ColumnTitle>Channel</ColumnTitle>,
            dataIndex: 'channels',
            key: 'channels',
            render: (_, subscription: DataHubSubscription) => (
                <ChannelColumn isPersonal={isPersonal} subscription={subscription} settingsChannel={settingsChannel} />
            ),
        },
        ...(ENABLE_UPSTREAM_NOTIFICATIONS
            ? [
                  {
                      title: <ColumnTitle>Subscribed to Upstreams</ColumnTitle>,
                      dataIndex: 'upstreams',
                      key: 'upstreams',
                      render: (_, subscription: DataHubSubscription) => <UpstreamsColumn subscription={subscription} />,
                  },
              ]
            : []),
        {
            title: <ColumnTitle>Subscribed Since</ColumnTitle>,
            dataIndex: 'since',
            key: 'since',
            render: (_, subscription: DataHubSubscription) => <SubscribedSinceColumn subscription={subscription} />,
        },
        {
            title: <ColumnTitle style={{ float: 'right', paddingRight: '8px' }}>Edit</ColumnTitle>,
            dataIndex: 'edit',
            key: 'edit',
            render: (_, subscription: DataHubSubscription) => (
                <EditSubscriptionColumn
                    subscription={subscription}
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
                    columns={subscriptionTableColumns as ColumnsType<any>}
                    dataSource={subscriptions}
                    rowKey="urn"
                    loading={loading ? { indicator: <LoadingOutlined /> } : false}
                    locale={
                        !loading
                            ? {
                                  emptyText: (
                                      <EmptyContainer>
                                          <EmptySimpleSvg />
                                          <EmptySubscriptionsText>
                                              You are not currently subscribed to any entities. Get started by
                                              subscribing to entities most relevant to you.
                                          </EmptySubscriptionsText>
                                      </EmptyContainer>
                                  ),
                              }
                            : undefined
                    }
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
