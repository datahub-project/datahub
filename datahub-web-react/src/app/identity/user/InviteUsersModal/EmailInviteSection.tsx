import { Button, Input } from '@components';
import React from 'react';

import { EmailInputSection, InputRow, SectionTitle } from '@app/identity/user/InviteUsersModal.components';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';

import { DataHubRole } from '@types';

type Props = {
    emailInput: string;
    emailInviteRole?: DataHubRole;
    noRoleText: string;
    emailValidationError?: string;
    onSelectEmailInviteRole: (roleUrn: string) => void;
    handleEmailInputChange: (value: string) => void;
    handleEmailInputKeyPress: (e: React.KeyboardEvent<HTMLInputElement>) => void;
    handleSendInvitations: () => void;
};

export default function EmailInviteSection({
    emailInput,
    emailInviteRole,
    noRoleText,
    emailValidationError,
    onSelectEmailInviteRole,
    handleEmailInputChange,
    handleEmailInputKeyPress,
    handleSendInvitations,
}: Props) {
    return (
        <EmailInputSection>
            <SectionTitle>Input emails</SectionTitle>
            <InputRow>
                <Input
                    label=""
                    placeholder="email1@address.com, email2@address.com"
                    value={emailInput}
                    onChange={(e) => handleEmailInputChange(e.target.value)}
                    onKeyPress={handleEmailInputKeyPress}
                    icon={{ icon: 'EnvelopeSimple', source: 'phosphor' }}
                    error={emailValidationError}
                    isInvalid={!!emailValidationError}
                    helperText="Add multiple team members by entering comma-separated email addresses"
                    style={{ flex: 1, maxWidth: '470px' }}
                />
                <SimpleSelectRole
                    selectedRole={emailInviteRole}
                    onRoleSelect={(role) => onSelectEmailInviteRole(role?.urn || '')}
                    placeholder={noRoleText}
                    size="md"
                />
                <Button onClick={handleSendInvitations} disabled={!emailInput.trim()} style={{ fontSize: '12px' }}>
                    Invite
                </Button>
            </InputRow>
        </EmailInputSection>
    );
}
