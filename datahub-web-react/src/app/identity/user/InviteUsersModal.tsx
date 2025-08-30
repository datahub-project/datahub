import { Avatar, Button, Icon, Input, Modal, Text, Tooltip } from '@components';
import { message } from 'antd';
import React, { useEffect } from 'react';

import {
    InputRow,
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
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import UserRecommendationsSection from '@app/identity/user/UserRecommendationsSection';

type Props = {
    open: boolean;
    onClose: () => void;
};

export default function InviteUsersModal({ open, onClose }: Props) {
    const {
        selectedRole,
        emailInviteRole,
        emailInput,
        invitedUsers,
        inviteLink,
        emailValidationError,
        noRoleText,
        recommendedUsers,
        onSelectRole,
        onSelectEmailInviteRole,
        createInviteToken,
        handleSendInvitations,
        handleEmailInputChange,
        handleEmailInputKeyPress,
        resetModalState,
    } = useInviteUsersModal();

    // Reset modal state when dialog opens
    useEffect(() => {
        if (open) {
            resetModalState();
        }
    }, [open, resetModalState]);

    return (
        <Modal
            width="649px"
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
                                onClick={() => createInviteToken(selectedRole?.urn)}
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
                {/* Invite Users Section */}
                <EmailInviteSection
                    emailInput={emailInput}
                    emailInviteRole={emailInviteRole}
                    noRoleText={noRoleText}
                    emailValidationError={emailValidationError}
                    onSelectEmailInviteRole={onSelectEmailInviteRole}
                    handleEmailInputChange={handleEmailInputChange}
                    handleEmailInputKeyPress={handleEmailInputKeyPress}
                    handleSendInvitations={handleSendInvitations}
                />
                {/* Invited Users List */}
                {invitedUsers.length > 0 && (
                    <InvitedUsersSection>
                        <InvitedUsersLabel>{invitedUsers.length} Invited</InvitedUsersLabel>
                        <InvitedUsersList>
                            {invitedUsers.map((user) => (
                                <InvitedUserItem key={user.email}>
                                    <Avatar name={user.email} size="sm" />
                                    <UserEmail>{user.email}</UserEmail>
                                    <Text size="sm" weight="medium" color="gray">
                                        {user.role?.name || 'No Role'}
                                    </Text>
                                </InvitedUserItem>
                            ))}
                        </InvitedUsersList>
                    </InvitedUsersSection>
                )}
                {/* User Recommendations Section */}
                {/* TODO: milestone 2 */}
                {recommendedUsers && recommendedUsers.length > 0 && (
                    <UserRecommendationsSection recommendedUsers={recommendedUsers} selectedRole={selectedRole} />
                )}
            </ModalSection>
        </Modal>
    );
}
