import React from 'react';
import styled from 'styled-components';

import { Button, PageTitle, Pill } from '@src/alchemy-components';

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
    overflow: hidden;
    color: ${(props) => props.theme.colors.textSecondary};

    &&& .ant-tabs-nav {
        margin-bottom: 0;
    }

    &&& .ant-tabs {
        display: flex;
        flex-direction: column;
    }

    &&& .ant-tabs-content-holder {
        flex: 1;
        min-height: 0;
    }

    &&& .ant-tabs-content {
        height: 100%;
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
    onCreateGroup: () => void;
};

export const ManageUsersAndGroupsHeader = ({
    version,
    canManageUsers,
    canManageServiceAccounts,
    activeTab,
    onInviteUsers,
    onCreateServiceAccount,
    onCreateGroup,
}: ManageUsersAndGroupsHeaderProps) => {
    const isServiceAccountsTab = activeTab === 'service-accounts';
    const isGroupsTab = activeTab === 'groups';

    const renderActionButton = () => {
        if (isServiceAccountsTab) {
            return (
                <Button
                    variant="filled"
                    disabled={!canManageServiceAccounts}
                    onClick={onCreateServiceAccount}
                    data-testid="create-service-account-button"
                >
                    Create Service Account
                </Button>
            );
        }
        if (isGroupsTab) {
            return (
                <Button variant="filled" onClick={onCreateGroup} data-testid="create-group-button">
                    Create Group
                </Button>
            );
        }
        return (
            <Button variant="filled" disabled={!canManageUsers} onClick={onInviteUsers}>
                Invite Users
            </Button>
        );
    };

    return (
        <PageHeaderContainer data-testid={`manage-users-groups-${version}`}>
            <HeaderLeft>
                <PageTitle
                    title="Manage Users &amp; Groups"
                    subTitle="View your DataHub users &amp; groups. Take administrative actions."
                />
            </HeaderLeft>
            <HeaderRight>{renderActionButton()}</HeaderRight>
        </PageHeaderContainer>
    );
};
