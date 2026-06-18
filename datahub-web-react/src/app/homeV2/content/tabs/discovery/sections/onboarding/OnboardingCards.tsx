import { Button, Card } from '@components';
import { Globe } from '@phosphor-icons/react/dist/csr/Globe';
import { Plugs } from '@phosphor-icons/react/dist/csr/Plugs';
import { UserPlus } from '@phosphor-icons/react/dist/csr/UserPlus';
import React, { useContext, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import { useTheme } from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import ViewInviteTokenModal from '@app/identity/user/ViewInviteTokenModal';
import OnboardingContext from '@app/onboarding/OnboardingContext';
import { HOME_PAGE_ONBOARDING_CARDS_ID } from '@app/onboarding/config/HomePageOnboardingConfig';
import { PageRoutes } from '@conf/Global';
import { useGetPlatforms } from '@src/app/homeV2/content/tabs/discovery/sections/platform/useGetPlatforms';

export const OnboardingCards = () => {
    const { t } = useTranslation('home.v2');
    const { t: tc } = useTranslation('common.actions');
    const theme = useTheme();
    const { user, platformPrivileges } = useUserContext();
    const { platforms, loading } = useGetPlatforms();
    const { isUserInitializing } = useContext(OnboardingContext);
    const [isViewingInviteToken, setIsViewingInviteToken] = useState(false);

    if (loading || platforms?.length || isUserInitializing || !user) {
        return null;
    }

    // We use manage policies here because this determines whether users can invite other users
    // with particular roles.
    const canManageUsers = platformPrivileges?.managePolicies;

    const openInviteUsers = () => {
        setIsViewingInviteToken(true);
    };

    return (
        <div style={{ display: 'flex', gap: '16px' }} id={HOME_PAGE_ONBOARDING_CARDS_ID}>
            <Link to={`${PageRoutes.INGESTION}`}>
                <Card
                    icon={<Plugs color={theme.colors.icon} size={32} />}
                    title={t('onboarding.addDataSourcesTitle')}
                    subTitle={t('onboarding.addDataSourcesSubtitle')}
                    button={<Button variant="text">{tc('add')}</Button>}
                />
            </Link>
            {canManageUsers ? (
                <Card
                    icon={<UserPlus color={theme.colors.icon} size={32} />}
                    title={t('onboarding.inviteUsersTitle')}
                    subTitle={t('onboarding.inviteUsersSubtitle')}
                    onClick={openInviteUsers}
                    button={<Button variant="text">{tc('invite')}</Button>}
                />
            ) : null}
            <Link to={`${PageRoutes.DOMAINS}?create=true`}>
                <Card
                    icon={<Globe color={theme.colors.icon} size={32} />}
                    title={t('onboarding.addDomainsTitle')}
                    subTitle={t('onboarding.addDomainsSubtitle')}
                    button={<Button variant="text">{tc('add')}</Button>}
                />
            </Link>
            <ViewInviteTokenModal open={isViewingInviteToken} onClose={() => setIsViewingInviteToken(false)} />
        </div>
    );
};
