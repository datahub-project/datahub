import { Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { GroupList } from '@app/identity/group/GroupList';
import { ServiceAccountList } from '@app/identity/serviceAccount';
import { UserList } from '@app/identity/user/UserList';
import { RoutedTabs } from '@app/shared/RoutedTabs';

const PageContainer = styled.div`
    padding-top: 20px;
    width: 100%;
    overflow: auto;
    flex: 1;
    display: flex;
    flex-direction: column;
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

const Content = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
    &&& .ant-tabs-nav {
        margin: 0;
    }
    color: ${(props) => props.theme.colors.text};
    // height: calc(100vh - 60px);

    &&& .ant-tabs > .ant-tabs-nav .ant-tabs-nav-wrap {
        padding-left: 28px;
    }
`;

enum TabType {
    Users = 'Users',
    Groups = 'Groups',
    ServiceAccounts = 'Service Accounts',
}

interface Props {
    version?: string; // used to help with cypress tests bouncing between versions. wait till correct version loads
}

export const ManageIdentities = ({ version }: Props) => {
    const { t } = useTranslation('entity.identity');

    /**
     * Determines which view should be visible: users, groups, or service accounts list.
     */
    const authenticatedUser = useUserContext();
    const canManageServiceAccounts = authenticatedUser?.platformPrivileges?.manageServiceAccounts || false;

    const getTabs = () => {
        const baseTabs = [
            {
                name: t('tabs.users'),
                path: TabType.Users.toLocaleLowerCase(),
                content: <UserList />,
                display: {
                    enabled: () => true,
                },
            },
            {
                name: t('tabs.groups'),
                path: TabType.Groups.toLocaleLowerCase(),
                content: <GroupList />,
                display: {
                    enabled: () => true,
                },
            },
        ];

        if (canManageServiceAccounts) {
            baseTabs.push({
                name: t('tabs.serviceAccounts'),
                path: 'service-accounts',
                content: <ServiceAccountList />,
                display: {
                    enabled: () => true,
                },
            });
        }

        return baseTabs;
    };

    const defaultTabPath = getTabs() && getTabs()?.length > 0 ? getTabs()[0].path : '';
    const onTabChange = () => null;

    return (
        <PageContainer>
            <PageHeaderContainer data-testid={`manage-users-groups-${version}`}>
                <PageTitle level={3}>{t('pageTitle')}</PageTitle>
                <Typography.Paragraph type="secondary">{t('pageSubTitle')}</Typography.Paragraph>
            </PageHeaderContainer>
            <Content>
                <RoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />
            </Content>
        </PageContainer>
    );
};
