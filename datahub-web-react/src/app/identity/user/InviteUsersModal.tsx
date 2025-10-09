import { Avatar, Button, Icon, Input, Modal, Text, Tooltip } from '@components';
import { message } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import ButtonTabs from '@app/homeV3/modules/shared/ButtonTabs/ButtonTabs';
import { Tab } from '@app/homeV3/modules/shared/ButtonTabs/types';
import {
    InputRow,
    InviteUsersTabsSection,
    InvitedUserItem,
    InvitedUsersLabel,
    InvitedUsersList,
    InvitedUsersSection,
    ModalSection,
    SectionTitle,
    UserEmail,
} from '@app/identity/user/InviteUsersModal.components';
import { useInviteUsersModal } from '@app/identity/user/InviteUsersModal.hooks';
import EmailInviteSection from '@app/identity/user/InviteUsersModal/EmailInviteSection';
import OrDividerComponent from '@app/identity/user/InviteUsersModal/OrDividerComponent';
import RecommendedUsersList from '@app/identity/user/RecommendedUsersList';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import {
    addToGlobalInvitedUsers,
    getHasInvitedYet,
    resetHasInvitedYet,
} from '@app/identity/user/inviteUsersGlobalState';

import { CorpUser, DataHubRole, SortOrder } from '@types';

type InvitationStatus = 'pending' | 'success' | 'failed';

type RecommendedUserState = {
    status: InvitationStatus;
    role?: DataHubRole;
};

type Props = {
    open: boolean;
    onClose: () => void;
};

const MAX_RECOMMENDED_USERS = 6;
// always fetch exactly 6, get fresh ones on reopen

const INVITE_TAB_KEYS = {
    VIA_EMAIL: 'via-email',
    RECOMMENDED: 'recommended',
} as const;

export default function InviteUsersModal({ open, onClose }: Props) {
    const {
        selectedRole,
        emailInviteRole,
        emailInput,
        parsedEmails,
        invitedUsers,
        inviteLink,
        emailValidationError,
        noRoleText,
        recommendedUsers,
        totalRecommendedUsers,
        onSelectRole,
        onSelectEmailInviteRole,
        createInviteToken,
        handleSendInvitations,
        handleEmailInputKeyPress,
        handleEmailsChange,
        resetModalState,
        emailInvitations,
        refetchRecommendations,
    } = useInviteUsersModal({
        limit: MAX_RECOMMENDED_USERS, // Fetch exactly the number we need
        sortInput: {
            sortCriterion: {
                field: 'userUsageTotalPast30DaysFeature',
                sortOrder: SortOrder.Descending,
            },
        },
        selectedPlatforms: [],
        modalOpen: open, // Only load recommendations when modal is open
    });

    // Track invitation status for recommended users
    const [recommendedUserStates, setRecommendedUserStates] = useState<Record<string, RecommendedUserState>>({});
    // Track users that have been successfully invited and should be hidden
    const [hiddenUsers, setHiddenUsers] = useState<Set<string>>(new Set());
    // Track which tab the user is currently viewing
    const [activeTab, setActiveTab] = useState<string>(INVITE_TAB_KEYS.VIA_EMAIL);

    // Reset modal state when dialog opens and advance recommendation index
    useEffect(() => {
        if (open) {
            resetModalState();
            setRecommendedUserStates({}); // Reset recommended user states
            setHiddenUsers(new Set()); // Reset hidden users
            setActiveTab(INVITE_TAB_KEYS.VIA_EMAIL); // Reset to default tab

            // Only refetch if user has invited someone (gamification approach)
            if (getHasInvitedYet()) {
                refetchRecommendations?.();
                resetHasInvitedYet(); // Reset flag after refetch
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [open, resetModalState]);

    const handleInviteRecommendedUser = useCallback(
        async (user: CorpUser, role?: DataHubRole) => {
            if (!role) {
                message.error('Please select a role before sending invitation');
                return false;
            }

            // Get user's email
            const userEmail = user.info?.email || user.properties?.email || user.username;
            if (!userEmail) {
                message.error('No email found for this user');
                return false;
            }

            try {
                // Update status to pending (for loading state if needed)
                setRecommendedUserStates((prev) => ({
                    ...prev,
                    [user.urn]: { status: 'pending', role },
                }));

                // Send invitation
                const success = await emailInvitations.sendInvitationToEmail(user.username, role);

                // Update status based on result
                setRecommendedUserStates((prev) => ({
                    ...prev,
                    [user.urn]: { status: success ? 'success' : 'failed', role },
                }));

                // Add to global tracking for successful invitations
                if (success) {
                    // Add to global invited users to persist across modal sessions
                    const identifiers = [user.urn];
                    const currentUserEmail = user.info?.email || user.properties?.email || user.username;
                    if (currentUserEmail && currentUserEmail !== user.urn) {
                        identifiers.push(currentUserEmail);
                    }
                    addToGlobalInvitedUsers(identifiers);

                    setTimeout(() => {
                        setHiddenUsers((prev) => new Set([...prev, user.urn]));
                    }, 3000);
                }

                return success;
            } catch (error) {
                // Update status to failed
                setRecommendedUserStates((prev) => ({
                    ...prev,
                    [user.urn]: { status: 'failed', role },
                }));

                // Track error event
                analytics.event({
                    type: EventType.InviteUserErrorEvent,
                    roleUrn: role.urn,
                    emailList: [userEmail],
                    inviteMethod: 'recommended_user',
                    errorMessage: error instanceof Error ? error.message : 'Unknown error',
                });

                message.error('Invitation failed');
                console.error('Failed to invite recommended user:', error);
                return false;
            }
        },
        [emailInvitations],
    );

    // Calculate how many displayable recommended users we have after filtering
    const validUsers = recommendedUsers.filter((user) => {
        // Don't show hidden users
        if (hiddenUsers.has(user.urn)) return false;

        return true;
    });

    // Gamification: show fewer slots as users get invited successfully (matches RecommendedUsersList)
    const invitedCount = Object.keys(recommendedUserStates).filter(
        (urn) => recommendedUserStates[urn]?.status === 'success',
    ).length;
    const availableSlots = Math.max(0, MAX_RECOMMENDED_USERS - invitedCount);

    // Only show the tab if there are displayable users after gamification
    const displayableRecommendedUsers = validUsers.slice(0, availableSlots);

    // Create tabs for the invite section
    const inviteTabs: Tab[] = [
        {
            key: INVITE_TAB_KEYS.VIA_EMAIL,
            label: 'Via Email',
            content: (
                <>
                    <EmailInviteSection
                        emailInput={emailInput}
                        parsedEmails={parsedEmails}
                        emailInviteRole={emailInviteRole}
                        noRoleText={noRoleText}
                        emailValidationError={emailValidationError}
                        onSelectEmailInviteRole={onSelectEmailInviteRole}
                        handleEmailInputKeyPress={handleEmailInputKeyPress}
                        handleSendInvitations={handleSendInvitations}
                        onEmailsChange={handleEmailsChange}
                    />

                    {invitedUsers.length > 0 && (
                        <InvitedUsersSection>
                            <InvitedUsersLabel>{invitedUsers.length} Invited</InvitedUsersLabel>
                            <InvitedUsersList>
                                {invitedUsers.map((user) => (
                                    <InvitedUserItem key={user.email}>
                                        <Avatar name={user.email} size="lg" />
                                        <UserEmail>{user.email}</UserEmail>
                                        <Text size="sm" weight="medium" color="gray">
                                            {user.role?.name || 'No Role'}
                                        </Text>
                                    </InvitedUserItem>
                                ))}
                            </InvitedUsersList>
                        </InvitedUsersSection>
                    )}
                </>
            ),
        },
        // Only show Recommended tab if there are users OR if user is currently on it
        // This allows empty state to show when user invites all, but hides tab when switching away
        ...(displayableRecommendedUsers.length > 0 || activeTab === INVITE_TAB_KEYS.RECOMMENDED
            ? [
                  {
                      key: INVITE_TAB_KEYS.RECOMMENDED,
                      label: 'Recommended',
                      content: (
                          <RecommendedUsersList
                              recommendedUsers={recommendedUsers}
                              totalRecommendedUsers={totalRecommendedUsers}
                              maxDisplayUsers={MAX_RECOMMENDED_USERS}
                              selectedRole={selectedRole}
                              onInviteUser={handleInviteRecommendedUser}
                              userStates={recommendedUserStates}
                              hiddenUsers={hiddenUsers}
                              onClose={onClose}
                          />
                      ),
                  },
              ]
            : []),
    ];

    return (
        <Modal
            width="660px"
            footer={null}
            title="Invite Users"
            subtitle="Add colleagues to your DataHub workspace."
            open={open}
            onCancel={onClose}
            buttons={[]}
        >
            <ModalSection>
                {/* Share Link Section */}
                <div>
                    <SectionTitle>Share Link</SectionTitle>
                    <InputRow>
                        <Input
                            className="meticulous-ignore"
                            label=""
                            value={inviteLink}
                            readOnly
                            placeholder="Invite link will appear here"
                            icon={{ icon: 'LinkSimple', source: 'phosphor' }}
                            helperText="Anyone with this link can join DataHub. Links stay active until refreshed"
                        />
                        <SimpleSelectRole
                            selectedRole={selectedRole}
                            onRoleSelect={(role) => onSelectRole(role?.urn || '')}
                            placeholder={noRoleText}
                            size="md"
                            width="fit-content"
                        />
                        <Tooltip title="Refresh">
                            <Button
                                className="refresh-btn"
                                variant="text"
                                onClick={() => createInviteToken(selectedRole?.urn, EventType.RefreshInviteLinkEvent)}
                                style={{
                                    padding: '4px',
                                    width: '32px',
                                    height: '32px',
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'center',
                                }}
                            >
                                <Icon icon="ArrowClockwise" source="phosphor" size="xl" />
                            </Button>
                        </Tooltip>
                        <Button
                            onClick={async () => {
                                try {
                                    await navigator.clipboard.writeText(inviteLink);
                                    message.success('Copied invite link to clipboard');

                                    // Track copy invite link event
                                    analytics.event({
                                        type: EventType.ClickCopyInviteLinkEvent,
                                        roleUrn: selectedRole?.urn || '',
                                    });
                                } catch (error) {
                                    message.error('Failed to copy invite link to clipboard');
                                }
                            }}
                            variant="secondary"
                            style={{ fontSize: '12px' }}
                        >
                            Copy
                        </Button>
                    </InputRow>
                </div>
                <OrDividerComponent />
                {/* Invite Users Section with Tabs */}
                <InviteUsersTabsSection>
                    <SectionTitle>Invite Users</SectionTitle>
                    <ButtonTabs tabs={inviteTabs} defaultKey="via-email" onTabClick={setActiveTab} />
                </InviteUsersTabsSection>
            </ModalSection>
        </Modal>
    );
}
