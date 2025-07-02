import { Button, Card, colors } from '@components';
import { Globe, Plugs, UserPlus } from '@phosphor-icons/react';
import React, { useContext, useState } from 'react';
import { Link } from 'react-router-dom';

import { useUserContext } from '@app/context/useUserContext';
import ViewInviteTokenModal from '@app/identity/user/ViewInviteTokenModal';
import OnboardingContext from '@app/onboarding/OnboardingContext';
import { HOME_PAGE_ONBOARDING_CARDS_ID } from '@app/onboarding/config/HomePageOnboardingConfig';
import { PageRoutes } from '@conf/Global';
import { useGetPlatforms } from '@src/app/homeV2/content/tabs/discovery/sections/platform/useGetPlatforms';

export const OnboardingCards = () => {
    const { user, platformPrivileges } = useUserContext();
    const { platforms, loading } = useGetPlatforms(user);
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
                    icon={<Plugs color={colors.gray[1800]} size={32} />}
                    title="Add Data Sources"
                    subTitle="Connect your data platforms"
                    button={<Button variant="text">Add</Button>}
                />
            </Link>
            {canManageUsers ? (
                <Card
                    icon={<UserPlus color={colors.gray[1800]} size={32} />}
                    title="Invite Users"
                    subTitle="Invite users to DataHub"
                    onClick={openInviteUsers}
                    button={<Button variant="text">Invite</Button>}
                />
            ) : null}
            <Link to={`${PageRoutes.DOMAINS}?create=true`}>
                <Card
                    icon={<Globe color={colors.gray[1800]} size={32} />}
                    title="Add Domains"
                    subTitle="Configure your data domains"
                    button={<Button variant="text">Add</Button>}
                />
            </Link>
            <ViewInviteTokenModal open={isViewingInviteToken} onClose={() => setIsViewingInviteToken(false)} />
        </div>
    );
};
