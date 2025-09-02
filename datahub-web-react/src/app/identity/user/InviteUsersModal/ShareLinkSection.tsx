import { Button, Icon, Input, SimpleSelect } from '@components';
import { message } from 'antd';
import React from 'react';

import { InputRow, SectionTitle } from '@app/identity/user/InviteUsersModal.components';

import { DataHubRole } from '@types';

type Props = {
    inviteLink: string;
    selectedRole?: DataHubRole;
    roleSelectOptions: Array<{ value: string; label: string }>;
    noRoleText: string;
    onSelectRole: (roleUrn: string) => void;
    createInviteToken: (roleUrn?: string) => void;
};

export default function ShareLinkSection({
    inviteLink,
    selectedRole,
    roleSelectOptions,
    noRoleText,
    onSelectRole,
    createInviteToken,
}: Props) {
    return (
        <div>
            <SectionTitle>Share Link</SectionTitle>
            <InputRow>
                <Input
                    label=""
                    value={inviteLink}
                    readOnly
                    placeholder="Invite link will appear here"
                    icon={{ icon: 'LinkSimple', source: 'phosphor' }}
                />
                <SimpleSelect
                    onUpdate={(values) => onSelectRole(values[0] || '')}
                    options={roleSelectOptions}
                    placeholder={noRoleText}
                    values={selectedRole?.urn ? [selectedRole.urn] : []}
                    size="md"
                    width="fit-content"
                />
                <Button
                    className="refresh-btn"
                    variant="outline"
                    onClick={() => createInviteToken(selectedRole?.urn)}
                    style={{
                        fontSize: '12px',
                        padding: '4px',
                        width: '32px',
                        height: '32px',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                    }}
                >
                    <Icon icon="ArrowClockwise" source="phosphor" size="sm" />
                </Button>
                <Button
                    onClick={async () => {
                        try {
                            await navigator.clipboard.writeText(inviteLink);
                            message.success('Copied invite link to clipboard');
                        } catch (error) {
                            console.error('Failed to copy invite link to clipboard:', error);
                            message.error('Failed to copy invite link. Please try again or copy manually.');
                        }
                    }}
                    style={{ fontSize: '12px' }}
                >
                    Copy
                </Button>
            </InputRow>
        </div>
    );
}
