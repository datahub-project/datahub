import React from 'react';
import dayjs from 'dayjs';
import styled from 'styled-components/macro';
import { Empty, Table, Typography } from 'antd';
import { EntityColumn } from './table/EntityColumn';
import { UpstreamsColumn } from './table/UpstreamsColumn';
import { SubscribedSinceColumn } from './table/SubscribedSinceColumn';
import { EditSubscriptionColumn } from './table/EditSubscriptionColumn';
import { ANTD_GRAY } from '../../../entity/shared/constants';

const SubscriptionsTitle = styled(Typography.Title)`
    margin-bottom: 12px;
`;

const SubscriptionsContainer = styled.div`
    margin-top: 12px;
    display: flex;
    flex-direction: column;
    gap: 20px;
`;

const SubscriptionsTable = styled(Table)`
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 8px;
    margin-right: 24px;
    align-self: flex-end;
`;

const ColumnTitle = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 700;
    color: rgba(0, 0, 0, 0.88);
`;

type Props = {
    isPersonal: boolean;
};

/**
 * Component used for managing actor subscriptions.
 */
export const ManageActorSubscriptions = ({ isPersonal }: Props) => {
    const fakeData = [
        {
            entityName: 'snowflake_pet_profile_etl',
            entityType: 'Dashboard',
            platform: 'looker',
            subscribedToUpstreams: true,
            numUpstreams: 8,
            subscribedSince: dayjs().subtract(1, 'month').valueOf(),
        },
        {
            entityName: 'pet_profile',
            entityType: 'Dataset',
            platform: 'snowflake',
            subscribedToUpstreams: false,
            numUpstreams: 2,
            subscribedSince: dayjs().subtract(2, 'month').valueOf(),
        },
    ];
    const pageTitle = isPersonal ? 'My Subscriptions' : 'Group Subscriptions';
    const subscriptionTableColumns = [
        {
            title: <ColumnTitle>Entity</ColumnTitle>,
            dataIndex: 'Entity',
            key: 'entity',
            sorter: (a: any, b: any) => a.entityName.localeCompare(b.entityName),
            render: (_, record: any) => <EntityColumn record={record} />,
        },
        {
            title: <ColumnTitle>Subscribed to Upstreams</ColumnTitle>,
            dataIndex: 'upstreams',
            key: 'upstreams',
            sorter: (a: any, b: any) => a.numUpstreams - b.numUpstreams,
            render: (_, record: any) => <UpstreamsColumn record={record} />,
        },
        {
            title: <ColumnTitle>Subscribed Since</ColumnTitle>,
            dataIndex: 'since',
            key: 'since',
            sorter: (a: any, b: any) => a.subscribedSince - b.subscribedSince,
            render: (_, record: any) => <SubscribedSinceColumn record={record} />,
        },
        {
            title: <ColumnTitle>Edit Subscription</ColumnTitle>,
            dataIndex: 'edit',
            key: 'edit',
            render: (_) => <EditSubscriptionColumn />,
        },
    ];

    return (
        <>
            <SubscriptionsTitle level={3}>{pageTitle}</SubscriptionsTitle>
            <SubscriptionsTable
                columns={subscriptionTableColumns}
                dataSource={fakeData}
                rowKey="urn"
                locale={{
                    emptyText: <Empty description="No Subscriptions" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                pagination={false}
            />
            <SubscriptionsContainer />
        </>
    );
};
