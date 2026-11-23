import { Button, EmptyState, PageTitle } from '@components';
import { Stack } from '@phosphor-icons/react/dist/csr/Stack';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { useUserContext } from '@app/context/useUserContext';
import { EnvironmentList } from '@app/settingsV2/environments/EnvironmentList';
import { useAppConfig } from '@app/useAppConfig';

const PageContainer = styled.div`
    padding: 16px 20px;
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
    gap: 16px;
`;

const PageHeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

const HeaderRight = styled.div`
    display: flex;
    align-items: center;
`;

/**
 * Settings page for custom environments (customFabricType entities): list, create, edit, deprecate
 * and delete. Shown only when custom environments are enabled and the user can manage them.
 */
export const ManageEnvironments = () => {
    const { t } = useTranslation('settings.environments');
    const { config } = useAppConfig();
    const { platformPrivileges } = useUserContext();
    const [showBuilder, setShowBuilder] = useState(false);

    const enabled = !!config.featureFlags.customFabricTypesEnabled;
    const canManage = !!platformPrivileges?.manageCustomFabricTypes;

    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle title={t('pageTitle')} subTitle={t('pageSubTitle')} />
                {enabled && canManage && (
                    <HeaderRight>
                        <Button
                            variant="filled"
                            onClick={() => setShowBuilder(true)}
                            data-testid="create-environment-button"
                        >
                            {t('createEnvironment')}
                        </Button>
                    </HeaderRight>
                )}
            </PageHeaderContainer>
            {enabled ? (
                <EnvironmentList showBuilder={showBuilder} setShowBuilder={setShowBuilder} canManage={canManage} />
            ) : (
                <EmptyState
                    title={t('disabledTitle')}
                    description={t('disabledDescription')}
                    icon={Stack}
                    style={{ flex: 1, justifyContent: 'center' }}
                />
            )}
        </PageContainer>
    );
};
