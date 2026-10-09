import { Button, PageTitle } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { POLICIES_CREATE_POLICY_ID } from '@app/onboarding/config/PoliciesOnboardingConfig';
import { ManageDataAccessRoles } from '@app/permissions/dataAccessRoles/ManageDataAccessRoles';
import { ManagePolicies } from '@app/permissions/policy/ManagePolicies';
import { ManageRoles } from '@app/permissions/roles/ManageRoles';
import { AlchemyRoutedTabs } from '@app/shared/AlchemyRoutedTabs';
import { useAppConfig } from '@app/useAppConfig';

const PageContainer = styled.div`
    padding: 16px 20px;
    width: 100%;
    flex: 1;
    display: flex;
    gap: 16px;
    flex-direction: column;
    overflow: hidden;
`;

const PageHeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

const HeaderLeft = styled.div`
    display: flex;
    flex-direction: column;
`;

const HeaderRight = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
`;

const Content = styled.div`
    min-height: 0;
    display: flex;
    flex: 1;
    flex-direction: column;
    overflow: hidden;

    &&& .ant-tabs-nav {
        margin-bottom: 0;
    }
`;

enum TabType {
    DataAccessRoles = 'data-access-roles',
    Roles = 'Roles',
    Policies = 'Policies',
}

export const ManagePermissions = () => {
    const { t } = useTranslation('settings.permissions');
    const location = useLocation();
    const { config } = useAppConfig();
    const showAccessManagement = config?.featureFlags?.showAccessManagement;
    const createPolicyRef = useRef<() => void>(() => {});
    const registerCreatePolicy = useCallback((fn: () => void) => {
        createPolicyRef.current = fn;
    }, []);

    const isPoliciesTab = location.pathname.includes('/policies');

    const getTabs = () => {
        return [
            {
                name: t('roles.tab'),
                path: TabType.Roles.toLocaleLowerCase(),
                content: <ManageRoles />,
                display: {
                    enabled: () => true,
                },
            },
            {
                name: t('policiesTab'),
                path: TabType.Policies.toLocaleLowerCase(),
                content: <ManagePolicies onRegisterCreatePolicy={registerCreatePolicy} />,
                display: {
                    enabled: () => true,
                },
            },
            {
                name: t('dataAccessRolesTab'),
                path: TabType.DataAccessRoles,
                content: <ManageDataAccessRoles />,
                display: {
                    enabled: () => showAccessManagement,
                },
            },
        ];
    };

    const enabledTabs = getTabs().filter((tab) => tab.display?.enabled() !== false);
    const defaultTabPath = enabledTabs.length > 0 ? enabledTabs[0].path : '';

    return (
        <PageContainer>
            <PageHeaderContainer>
                <HeaderLeft>
                    <PageTitle title={t('pageTitle')} subTitle={t('pageSubTitle')} />
                </HeaderLeft>
                {isPoliciesTab && (
                    <HeaderRight>
                        <Button
                            id={POLICIES_CREATE_POLICY_ID}
                            variant="filled"
                            icon={{ icon: Plus }}
                            onClick={() => createPolicyRef.current()}
                            data-testid="add-policy-button"
                        >
                            {t('createNewPolicy')}
                        </Button>
                    </HeaderRight>
                )}
            </PageHeaderContainer>
            <Content>
                <AlchemyRoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} />
            </Content>
        </PageContainer>
    );
};
