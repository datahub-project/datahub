import { Tabs, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { SearchablePage } from '../search/SearchablePage';

const PageContainer = styled.div`
    padding-top: 20px;
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 24px;
    }
`;

const PageTitle = styled(Typography.Title)`
    && {
        margin-bottom: 24px;
    }
`;

const StyledTabs = styled(Tabs)`
    &&& .ant-tabs-nav {
        margin-bottom: 0;
        padding-left: 28px;
    }
`;

const Tab = styled(Tabs.TabPane)`
    font-size: 14px;
    line-height: 22px;
`;

const ListContainer = styled.div``;

export const ManageDomainsPage = () => {
    return (
        <SearchablePage>
            <PageContainer>
                <PageHeaderContainer>
                    <PageTitle level={3}>Manage Domains</PageTitle>
                    <Typography.Paragraph type="secondary">
                        View your DataHub Domains. Take administrative actions.
                    </Typography.Paragraph>
                </PageHeaderContainer>
                <ListContainer></ListContainer>
            </PageContainer>
        </SearchablePage>
    );
};
