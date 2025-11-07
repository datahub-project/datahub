import { Button } from '@components';
import React from 'react';

import { EmailInputSection, InputRow, SectionTitle } from '@app/identity/user/InviteUsersModal.components';
import EmailPillInput from '@app/identity/user/InviteUsersModal/EmailPillInput';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';

import { DataHubRole } from '@types';

type Props = {
    emailInput: string;
    parsedEmails?: string[];
    emailInviteRole?: DataHubRole;
    noRoleText: string;
    emailValidationError?: string;
    onSelectEmailInviteRole: (roleUrn: string) => void;
    handleEmailInputKeyPress: (e: React.KeyboardEvent<HTMLInputElement>) => void;
    handleSendInvitations: () => void;
    onEmailsChange: (emails: string[]) => void;
};

export default function EmailInviteSection({
    emailInput,
    parsedEmails = [],
    emailInviteRole,
    noRoleText,
    emailValidationError,
    onSelectEmailInviteRole,
    handleEmailInputKeyPress,
    handleSendInvitations,
    onEmailsChange,
}: Props) {
    return (
        <EmailInputSection>
            <SectionTitle>Input emails</SectionTitle>
            <InputRow>
                <EmailPillInput
                    onEmailsChange={onEmailsChange}
                    onKeyPress={handleEmailInputKeyPress}
                    placeholder="email1@address.com, email2@address.com"
                    error={emailValidationError}
                />
                <SimpleSelectRole
                    selectedRole={emailInviteRole}
                    onRoleSelect={(role) => onSelectEmailInviteRole(role?.urn || '')}
                    placeholder={noRoleText}
                    size="md"
                />
                <Button
                    onClick={handleSendInvitations}
                    disabled={parsedEmails.length === 0 && !emailInput.trim()}
                    style={{ fontSize: '12px' }}
                >
                    Invite
                </Button>
            </InputRow>
        </EmailInputSection>
    );
}
