/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ViewsList } from '@app/entity/view/ViewsList';

const PageContainer = styled.div`
    padding-top: 20px;
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
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

const ListContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

/**
 * Component used for displaying the 'Manage Views' experience.
 */
export const ManageViews = () => {
    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle level={3}>Manage Views</PageTitle>
                <Typography.Paragraph type="secondary">
                    Create, edit, and remove your Views. Views allow you to save and share sets of filters for reuse
                    when browsing DataHub.
                </Typography.Paragraph>
            </PageHeaderContainer>
            <ListContainer>
                <ViewsList />
            </ListContainer>
        </PageContainer>
    );
};
