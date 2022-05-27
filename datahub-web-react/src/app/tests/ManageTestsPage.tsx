import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { SearchablePage } from '../search/SearchablePage';
import { TestsList } from './TestsList';

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
        margin-bottom: 12px;
    }
`;

const ListContainer = styled.div``;

export const ManageTestsPage = () => {
    return (
        <SearchablePage>
            <PageContainer>
                <PageHeaderContainer>
                    <PageTitle level={3}>Manage Tests</PageTitle>
                    <Typography.Paragraph type="secondary">
                        DataHub Tests allows you to continuously evaluate a set of conditions on the assets comprising
                        your Metadata Graph. <br />
                    </Typography.Paragraph>
                </PageHeaderContainer>
                <ListContainer>
                    <TestsList />
                </ListContainer>
            </PageContainer>
        </SearchablePage>
    );
};
