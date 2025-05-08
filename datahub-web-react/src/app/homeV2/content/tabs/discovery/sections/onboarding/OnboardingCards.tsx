import { Button, Card, colors } from '@components';
import { Globe, Plugs, UserPlus } from '@phosphor-icons/react';
import { useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import ViewInviteTokenModal from '@app/identity/user/ViewInviteTokenModal';
import { HOME_PAGE_ONBOARDING_CARDS_ID } from '@app/onboarding/config/HomePageOnboardingConfig';
import { PageRoutes } from '@conf/Global';

export const OnboardingCards = () => {
    const history = useHistory();
    const user = useUserContext();
    const [isViewingInviteToken, setIsViewingInviteToken] = useState(false);

    // We use manage policies here because this determines whether users can invite other users
    // with particular roles.
    const canManageUsers = user?.platformPrivileges?.managePolicies;

    const navigateToIngestion = () => {
        history.push({
            pathname: PageRoutes.INGESTION,
            search: '?create=true',
        });
    };

    const openInviteUsers = () => {
        setIsViewingInviteToken(true);
    };

    const navigateToDomains = () => {
        history.push({
            pathname: PageRoutes.DOMAINS,
            search: '?create=true',
        });
    };

    return (
        <div style={{ display: 'flex', gap: '16px' }} id={HOME_PAGE_ONBOARDING_CARDS_ID}>
            <Card
                width="33%"
                icon={<Plugs color={colors.gray[1800]} size={32} />}
                title="Add Data Sources"
                subTitle="Connect your data platforms"
                onClick={navigateToIngestion}
                button={<Button variant="text">Add</Button>}
            />
            {canManageUsers ? (
                <Card
                    width="33%"
                    icon={<UserPlus color={colors.gray[1800]} size={32} />}
                    title="Invite Users"
                    subTitle="Invite users to DataHub"
                    onClick={openInviteUsers}
                    button={<Button variant="text">Invite</Button>}
                />
            ) : null}
            <Card
                width="33%"
                icon={<Globe color={colors.gray[1800]} size={32} />}
                title="Add Domains"
                subTitle="Configure your data domains"
                onClick={navigateToDomains}
                button={<Button variant="text">Add</Button>}
            />
            <ViewInviteTokenModal open={isViewingInviteToken} onClose={() => setIsViewingInviteToken(false)} />
        </div>
    );
};
