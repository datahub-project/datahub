import { Button, Text, Tooltip } from '@components';
import { Divider, Modal, Typography, message } from 'antd';
import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import { checkIsSsoConfigured } from '@app/settingsV2/platform/sso/utils';
import { PageRoutes } from '@conf/Global';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import { useCreateInviteTokenMutation } from '@graphql/mutations.generated';
import { useGetInviteTokenQuery } from '@graphql/role.generated';
import { useGetSsoSettingsQuery } from '@graphql/settings.generated';
import { DataHubRole } from '@types';

const ModalSection = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const ModalSectionFooter = styled(Typography.Paragraph)`
    &&&& {
        padding: 0px;
        margin: 0px;
        margin-bottom: 4px;
    }
`;

const InviteLinkDiv = styled.div`
    margin-top: -12px;
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    gap: 10px;
    align-items: center;
`;

const CopyText = styled(Typography.Text)`
    display: flex;
    gap: 10px;
    align-items: center;
    flex: 1;
`;

type Props = {
    open: boolean;
    onClose: () => void;
};

export default function ViewInviteTokenModal({ open, onClose }: Props) {
    const baseUrl = window.location.origin;
    const [selectedRole, setSelectedRole] = useState<DataHubRole>();

    // Code related to getting or creating an invite token
    const { data: getInviteTokenData } = useGetInviteTokenQuery({
        skip: !open,
        variables: { input: { roleUrn: selectedRole?.urn } },
    });

    const [inviteToken, setInviteToken] = useState<string>(getInviteTokenData?.getInviteToken?.inviteToken || '');

    const [createInviteTokenMutation] = useCreateInviteTokenMutation();

    useEffect(() => {
        if (getInviteTokenData?.getInviteToken?.inviteToken) {
            setInviteToken(getInviteTokenData.getInviteToken.inviteToken);
        }
    }, [getInviteTokenData]);

    const createInviteToken = (roleUrn?: string) => {
        createInviteTokenMutation({
            variables: {
                input: {
                    roleUrn,
                },
            },
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.CreateInviteLinkEvent,
                        roleUrn,
                    });
                    setInviteToken(data?.createInviteToken?.inviteToken || '');
                    message.success('Generated new invite link');
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to create Invite Token for role ${selectedRole?.name} : \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };

    const inviteLink = `${baseUrl}${resolveRuntimePath(`${PageRoutes.SIGN_UP}?invite_token=${inviteToken}`)}`;

    const { data: ssoSettings } = useGetSsoSettingsQuery();
    const isSsoConfigured = checkIsSsoConfigured(ssoSettings?.globalSettings?.ssoSettings);

    return (
        <Modal width={950} footer={null} title="Share Invite Link" open={open} onCancel={onClose}>
            <ModalSection>
                <InviteLinkDiv>
                    <SimpleSelectRole
                        selectedRole={selectedRole}
                        onRoleSelect={setSelectedRole}
                        placeholder="No Role"
                        size="md"
                    />
                    <CopyText className="meticulous-ignore">
                        <pre>{inviteLink}</pre>
                    </CopyText>
                    <Tooltip title="Copy invite link.">
                        <Button
                            onClick={() => {
                                navigator.clipboard.writeText(inviteLink);
                                message.success('Copied invite link to clipboard');
                            }}
                        >
                            Copy
                        </Button>
                    </Tooltip>
                    <Tooltip title="Generate a new link. Any old links will no longer be valid.">
                        <Button
                            variant="outline"
                            onClick={() => {
                                createInviteToken(selectedRole?.urn);
                            }}
                        >
                            Refresh
                        </Button>
                    </Tooltip>
                </InviteLinkDiv>
                <ModalSectionFooter type="secondary">
                    Copy an invite link to send to your users. When they join, users will be automatically assigned to
                    the selected role.
                </ModalSectionFooter>

                {!isSsoConfigured && (
                    <>
                        <Divider />
                        <Text size="lg" weight="semiBold">
                            Or, set up Single Sign-On
                        </Text>
                        <Text color="gray" size="md" style={{ marginBottom: 12 }}>
                            Setting up SSO allows teammates within your organization to sign up with their existing
                            accounts.
                        </Text>
                        <Link to="/settings/sso">
                            <Button variant="secondary">Configure SSO</Button>
                        </Link>
                    </>
                )}
            </ModalSection>
        </Modal>
    );
}
