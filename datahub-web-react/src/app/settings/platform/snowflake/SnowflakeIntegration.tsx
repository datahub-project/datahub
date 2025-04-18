import React, { useState } from 'react';

import styled from 'styled-components';
import { DeleteOutlined, EditOutlined } from '@ant-design/icons';
import { Button, Divider, Typography, Alert, Image, Table } from 'antd';

import { useGetSearchResultsForMultipleQuery } from '../../../../graphql/search.generated';
import { EntityType } from '../../../../types.generated';

import snowflakeConfig from '../../../ingest/source/conf/snowflake/snowflake';
import { Message } from '../../../shared/Message';
import { PlatformIntegrationBreadcrumb } from '../PlatformIntegrationBreadcrumb';

import { PLATFORM_FILTER_NAME } from '../../../searchV2/utils/constants';
import { SnowflakeConnectionModal } from './Modal';

export const PLATFORM = 'urn:li:dataPlatform:snowflake';

const Page = styled.div`
    width: 100%;
    display: flex;
    justify-content: center;
`;

const ContentContainer = styled.div`
    padding-top: 20px;
    padding-right: 40px;
    padding-left: 40px;
    width: 100%;
`;

const Content = styled.div`
    display: flex;
    align-items: top;
    justify-content: space-between;

    & .actions-column {
        text-align: right;
    }
`;

const FlexContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
`;

const Title = styled.div`
    & h3 {
        margin: 0;
    }
`;

const CreateButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    flex: 1;
`;

const PlatformLogo = styled(Image)`
    max-height: 35px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const TableActions = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
`;

export const SnowflakeIntegration = () => {
    // Modal Mgmt
    const [modelKey, setModelKey] = useState<string | undefined>();
    const isModalVisible = (key: string) => modelKey === key;
    const closeModal = () => setModelKey(undefined);

    // Get list of connections
    const { data, loading, error } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.DatahubConnection],
                query: '*',
                start: 0,
                count: 50,
                orFilters: [{ and: [{ field: PLATFORM_FILTER_NAME, values: [PLATFORM] }] }],
            },
        },
    });

    const connections = data?.searchAcrossEntities?.searchResults?.map((result) => result.entity);

    const dataSource = connections?.map((connection: any) => ({
        key: connection?.urn,
        name: connection?.details?.name,
    }));

    const columns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
        },
        {
            title: 'Actions',
            key: 'actions',
            className: 'actions-column',
            width: '10%',
            render: (_: any, record: any) => (
                <TableActions>
                    <Button type="text" size="small" onClick={() => setModelKey(record.key)}>
                        <EditOutlined />
                    </Button>
                    <Button type="text" size="small" disabled>
                        <DeleteOutlined />
                    </Button>
                    <SnowflakeConnectionModal
                        title="Edit Snowflake Connection"
                        snowflakeConnectionId={record.key}
                        isModalVisible={isModalVisible(record.key)}
                        closeModal={closeModal}
                    />
                </TableActions>
            ),
        },
    ];

    return (
        <Page>
            <ContentContainer>
                <PlatformIntegrationBreadcrumb name="Snowflake" />
                <FlexContainer>
                    <div>
                        <PlatformLogo preview={false} src={snowflakeConfig.logoUrl} alt="slack-logo" />
                    </div>
                    <Title>
                        <Typography.Title level={3}>Snowflake</Typography.Title>
                        <Typography.Text type="secondary">Manage Snowflake connections for automations</Typography.Text>
                    </Title>
                    <CreateButtonContainer>
                        <Button type="primary" onClick={() => setModelKey('create')}>
                            Create Connection
                        </Button>
                    </CreateButtonContainer>
                </FlexContainer>
                <Divider />
                <Content>
                    {loading && <Message type="loading" content="Fetching connections…" style={{ marginTop: '10%' }} />}
                    {error && <Alert type="error" message={error || `Failed to fetch connections`} />}
                    {!loading && !error && (
                        <Table dataSource={dataSource} columns={columns} size="small" style={{ width: '100%' }} />
                    )}
                </Content>
            </ContentContainer>
            <SnowflakeConnectionModal
                title="Create Snowflake Connection"
                isModalVisible={isModalVisible('create')}
                closeModal={closeModal}
            />
        </Page>
    );
};
