import React from 'react';
import styled from 'styled-components';

import { Button, PageTitle, Pill } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';

export const PageContainer = styled.div`
    padding: 16px 20px;
    width: 100%;
    flex: 1;
    display: flex;
    gap: 16px;
    flex-direction: column;
    overflow: hidden;
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
    gap: 12px;
`;

export const Content = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    color: ${colors.gray[600]};

    &&& .ant-tabs-nav {
        margin-bottom: 0;
    }
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

type ManageUsersAndGroupsHeaderProps = {
    version?: string;
    canManageUsers: boolean;
    canManageServiceAccounts: boolean;
    activeTab: string;
    onInviteUsers: () => void;
    onCreateServiceAccount: () => void;
};

export const ManageUsersAndGroupsHeader = ({
    version,
    canManageUsers,
    canManageServiceAccounts,
    activeTab,
    onInviteUsers,
    onCreateServiceAccount,
}: ManageUsersAndGroupsHeaderProps) => {
    const isServiceAccountsTab = activeTab === 'service-accounts';

    return (
        <PageHeaderContainer data-testid={`manage-users-groups-${version}`}>
            <HeaderLeft>
                <PageTitle
                    title="Manage Users &amp; Groups"
                    subTitle="View your DataHub users &amp; groups. Take administrative actions."
                />
            </HeaderLeft>
            <HeaderRight>
                {isServiceAccountsTab ? (
                    <Button
                        variant="filled"
                        disabled={!canManageServiceAccounts}
                        onClick={onCreateServiceAccount}
                        data-testid="create-service-account-button"
                    >
                        Create Service Account
                    </Button>
                ) : (
                    <Button variant="filled" disabled={!canManageUsers} onClick={onInviteUsers}>
                        Invite Users
                    </Button>
                )}
            </HeaderRight>
        </PageHeaderContainer>
    );
};
