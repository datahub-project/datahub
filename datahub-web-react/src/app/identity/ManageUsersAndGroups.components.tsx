import React from 'react';
import styled from 'styled-components';

import { Button, PageTitle, Pill } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';

export const PageContainer = styled.div`
    padding: 16px 20px;
    width: 100%;
    overflow: auto;
    flex: 1;
    display: flex;
    gap: 20px;
    flex-direction: column;
`;

export const PageHeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

export const HeaderLeft = styled.div`
    display: flex;
    flex-direction: column;
`;

export const HeaderRight = styled.div`
    display: flex;
    align-items: center;
`;

export const Content = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: auto;
    color: ${colors.gray[600]};
`;

const TabTitle = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

type TabTitleWithCountProps = {
    name: string;
    count: number;
};

export const TabTitleWithCount = ({ name, count }: TabTitleWithCountProps) => (
    <TabTitle>
        {name}
        <Pill variant="filled" color="violet" label={count.toString()} />
    </TabTitle>
);

type ManageIdentitiesHeaderProps = {
    version?: string;
    canManagePolicies: boolean;
    onInviteUsers: () => void;
};

export const ManageUsersAndGroupsHeader = ({
    version,
    canManagePolicies,
    onInviteUsers,
}: ManageIdentitiesHeaderProps) => (
    <PageHeaderContainer data-testid={`manage-users-groups-${version}`}>
        <HeaderLeft>
            <PageTitle
                title="Manage Users &amp; Groups"
                subTitle="View your DataHub users &amp; groups. Take administrative actions."
            />
        </HeaderLeft>
        <HeaderRight>
            <Button variant="filled" isDisabled={!canManagePolicies} onClick={onInviteUsers}>
                Invite Users
            </Button>
        </HeaderRight>
    </PageHeaderContainer>
);
