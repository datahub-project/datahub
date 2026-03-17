import { Button, PageTitle } from '@components';
import React, { useCallback, useRef } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { POLICIES_CREATE_POLICY_ID } from '@app/onboarding/config/PoliciesOnboardingConfig';
import { ManagePolicies } from '@app/permissions/policy/ManagePolicies';
import { ManageRoles } from '@app/permissions/roles/ManageRoles';
import { AlchemyRoutedTabs } from '@app/shared/AlchemyRoutedTabs';

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
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    overflow: hidden;

    &&& .ant-tabs-nav {
        margin-bottom: 0;
    }
`;

enum TabType {
    Roles = 'Roles',
    Policies = 'Policies',
}
const ENABLED_TAB_TYPES = [TabType.Roles, TabType.Policies];

export const ManagePermissions = () => {
    const location = useLocation();
    const createPolicyRef = useRef<() => void>(() => {});
    const registerCreatePolicy = useCallback((fn: () => void) => {
        createPolicyRef.current = fn;
    }, []);

    const isPoliciesTab = location.pathname.includes('/policies');

    const getTabs = () => {
        return [
            {
                name: TabType.Roles,
                path: TabType.Roles.toLocaleLowerCase(),
                content: <ManageRoles />,
                display: {
                    enabled: () => true,
                },
            },
            {
                name: TabType.Policies,
                path: TabType.Policies.toLocaleLowerCase(),
                content: <ManagePolicies onRegisterCreatePolicy={registerCreatePolicy} />,
                display: {
                    enabled: () => true,
                },
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    const defaultTabPath = getTabs() && getTabs()?.length > 0 ? getTabs()[0].path : '';

    return (
        <PageContainer>
            <PageHeaderContainer>
                <HeaderLeft>
                    <PageTitle
                        title="Manage Permissions"
                        subTitle="View your DataHub permissions. Take administrative actions."
                    />
                </HeaderLeft>
                {isPoliciesTab && (
                    <HeaderRight>
                        <Button
                            id={POLICIES_CREATE_POLICY_ID}
                            variant="filled"
                            icon={{ icon: 'Plus', source: 'phosphor' }}
                            onClick={() => createPolicyRef.current()}
                            data-testid="add-policy-button"
                        >
                            Create new policy
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
