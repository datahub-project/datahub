import { Divider } from 'antd';
import React, { useMemo, useState } from 'react';

import { EmailInvitationService } from '@app/identity/user/EmailInvitationService';
import RecommendationsHeader from '@app/identity/user/RecommendationsHeader';
import UserRecommendationCard from '@app/identity/user/UserRecommendationCard';
import { RecommendationsSection } from '@app/identity/user/ViewInviteTokenModal.components';

import { useSendUserInvitationsMutation } from '@graphql/mutations.generated';
import { CorpUser, DataHubRole } from '@types';

interface Props {
    recommendedUsers: CorpUser[];
    selectedRole?: DataHubRole;
}

export default function UserRecommendationsSection({ recommendedUsers, selectedRole }: Props) {
    const [sendUserInvitationsMutation] = useSendUserInvitationsMutation();

    // Local role state for recommendations section
    const [localSelectedRole, setLocalSelectedRole] = useState<DataHubRole | undefined>(selectedRole);

    // Use local role if set, otherwise fall back to prop
    const activeRole = localSelectedRole || selectedRole;

    // Create service instance
    const emailService = new EmailInvitationService(sendUserInvitationsMutation);

    // Memoized list of users with valid email addresses
    const usersWithEmails = useMemo(() => {
        return recommendedUsers.filter(EmailInvitationService.hasValidEmail);
    }, [recommendedUsers]);

    const handleBulkSendInvitations = async () => {
        if (!activeRole) return;
        await emailService.sendBulkInvitations(usersWithEmails, activeRole);
    };

    const handleSendInvitationEmail = async (user: CorpUser) => {
        if (!activeRole) return;
        await emailService.sendSingleInvitation(user, activeRole);
    };

    if (recommendedUsers.length === 0) {
        return null;
    }

    return (
        <div>
            <Divider />
            <RecommendationsHeader
                totalUsers={recommendedUsers.length}
                usersWithEmails={usersWithEmails}
                selectedRole={activeRole}
                onRoleSelect={setLocalSelectedRole}
                onBulkSendInvitations={handleBulkSendInvitations}
            />
            <RecommendationsSection>
                {recommendedUsers.map((user) => (
                    <UserRecommendationCard key={user.urn} user={user} onSendInvitation={handleSendInvitationEmail} />
                ))}
            </RecommendationsSection>
        </div>
    );
}
