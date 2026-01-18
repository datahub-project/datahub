import React from 'react';
import styled from 'styled-components';

import { Button, Icon, PageTitle, Pill, Text } from '@src/alchemy-components';
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

const SsoWarningContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    background-color: ${colors.blue[0]};
    border-radius: 8px;
    color: ${colors.blue[1000]};
    padding: 8px;
`;

const SsoWarningContent = styled.div`
    display: flex;
    align-items: flex-start;
    gap: 12px;
`;

const SsoWarningText = styled.div`
    display: flex;
    flex-direction: column;
`;

type SsoWarningBannerProps = {
    onConfigureSso: () => void;
};

export const SsoWarningBanner = ({ onConfigureSso }: SsoWarningBannerProps) => (
    <SsoWarningContainer>
        <SsoWarningContent>
            <Icon
                icon="Info"
                source="phosphor"
                size="xl"
                weight="fill"
                style={{ marginTop: '3px', marginRight: '-8px' }}
            />
            <SsoWarningText>
                <Text weight="semiBold">Single Sign-On has not been enabled</Text>
                <Text size="sm">
                    Setting up SSO allows teammates within your organization to sign up with their existing accounts.
                </Text>
            </SsoWarningText>
        </SsoWarningContent>
        <Button
            variant="link"
            onClick={onConfigureSso}
            size="sm"
            style={{ color: colors.blue[1000], whiteSpace: 'nowrap' }}
        >
            Configure SSO
        </Button>
    </SsoWarningContainer>
);
