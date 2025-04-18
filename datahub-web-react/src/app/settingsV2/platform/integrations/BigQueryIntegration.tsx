import { Divider, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { BigQuery, useGetConnections } from '@app/connections';
import { PlatformIntegrationBreadcrumb } from '@app/settingsV2/platform/PlatformIntegrationBreadcrumb';

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

export const BigQueryIntegration = () => {
    const {
        components: { Logo, CreateButton, Table },
        constants: { PLATFORM_URN },
    } = BigQuery;

    // Fetch list of connections & setup refetch handler
    const { connections, loading, error, refetch } = useGetConnections({ platformUrn: PLATFORM_URN });
    const handleRefetch = (timeout = 3000) => {
        setTimeout(() => {
            refetch?.();
        }, timeout);
    };

    // Connections Mgmt Prop
    const connectionsProp = {
        data: connections,
        loading,
        error,
        refetch: handleRefetch,
    };

    return (
        <Page>
            <ContentContainer>
                <PlatformIntegrationBreadcrumb name="BigQuery" />
                <FlexContainer>
                    <div>
                        <Logo />
                    </div>
                    <Title>
                        <Typography.Title level={3}>BigQuery</Typography.Title>
                        <Typography.Text type="secondary">Manage BigQuery connections for automations</Typography.Text>
                    </Title>
                    <CreateButtonContainer>
                        <CreateButton connections={connectionsProp} />
                    </CreateButtonContainer>
                </FlexContainer>
                <Divider />
                <Content>
                    <Table connections={connectionsProp} />
                </Content>
            </ContentContainer>
        </Page>
    );
};
